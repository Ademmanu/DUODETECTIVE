#!/usr/bin/env python3
"""
Duplicate Message Monitor + Manual Reply System

Fixed monitor.py — added missing getallid_command and ensured callback routing,
friendly replies, and admin commands are present.

Drop this file in place of your current monitor.py and restart the service.
"""

import os
import asyncio
import logging
import functools
import base64
import json
from typing import Dict, List, Optional, Any, Set
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.errors import SessionPasswordNeededError, FloodWaitError
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
)
from database import Database
from webserver import start_server_thread, register_monitoring

logger = logging.getLogger("monitor")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# Environment vars
BOT_TOKEN = os.getenv("BOT_TOKEN")
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")

OWNER_IDS: Set[int] = set()
owner_env = os.getenv("OWNER_IDS", "").strip()
if owner_env:
    for part in owner_env.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            OWNER_IDS.add(int(part))
        except ValueError:
            logger.warning("Invalid OWNER_IDS value skipped: %s", part)

ALLOWED_USERS: Set[int] = set()
allowed_env = os.getenv("ALLOWED_USERS", "").strip()
if allowed_env:
    for part in allowed_env.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            ALLOWED_USERS.add(int(part))
        except ValueError:
            logger.warning("Invalid ALLOWED_USERS value skipped: %s", part)

MAX_CONCURRENT_USERS = int(os.getenv("MAX_CONCURRENT_USERS", "50"))
SEND_WORKER_COUNT = int(os.getenv("SEND_WORKER_COUNT", "6"))
SEND_QUEUE_MAXSIZE = int(os.getenv("SEND_QUEUE_MAXSIZE", "5000"))

db = Database()

# runtime state
user_clients: Dict[int, TelegramClient] = {}
login_states: Dict[int, Dict] = {}
task_creation_states: Dict[int, Dict[str, Any]] = {}
monitoring_cache: Dict[int, List[Dict]] = {}  # user_id -> monitoring tasks
handler_registered: Dict[int, Any] = {}
reply_waiting: Dict[int, Dict] = {}  # owner_id -> {user_id, chat_id, message_id}

# global send queue (used to send replies into chats)
send_queue: Optional[asyncio.Queue] = None
worker_tasks: List[asyncio.Task] = []
_send_workers_started = False
MAIN_LOOP: Optional[asyncio.AbstractEventLoop] = None

UNAUTHORIZED_MESSAGE = (
    "🚫 *Access Denied!*\n\n"
    "You are not authorized to use this system.\n\n"
    "📞 Call: `07089430305`\n"
    "🗨️ Message Dev: [HEMMY](https://t.me/justmemmy)"
)

# application global (set in main)
APP_INSTANCE = None

# helpers
async def db_call(func, *args, **kwargs):
    return await asyncio.to_thread(functools.partial(func, *args, **kwargs))


async def check_authorization(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    user_id = update.effective_user.id
    try:
        is_allowed_db = await db_call(db.is_user_allowed, user_id)
    except Exception:
        logger.exception("Error checking DB allowed users for %s", user_id)
        is_allowed_db = False
    is_allowed_env = (user_id in ALLOWED_USERS) or (user_id in OWNER_IDS)
    if not (is_allowed_db or is_allowed_env):
        if update.message:
            await update.message.reply_markdown_v2(UNAUTHORIZED_MESSAGE, disable_web_page_preview=True)
        elif update.callback_query:
            await update.callback_query.answer()
            await update.callback_query.message.reply_markdown_v2(UNAUTHORIZED_MESSAGE, disable_web_page_preview=True)
        return False
    return True


# ---------- Friendly UI handlers ----------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not await check_authorization(update, context):
        return

    user = await db_call(db.get_user, user_id)
    user_name = update.effective_user.first_name or "User"
    user_phone = user["phone"] if user and user["phone"] else "Not connected"
    is_logged_in = user and user["is_logged_in"]

    status_emoji = "🟢" if is_logged_in else "🔴"
    status_text = "Connected" if is_logged_in else "Not connected"

    text = (
        f"Hello *{user_name}* 👋\n\n"
        f"Phone: `{user_phone}`\n"
        f"Status: {status_emoji} *{status_text}*\n\n"
        "Quick actions:\n"
        "• 🟢 Connect / 🔴 Disconnect\n"
        "• 📋 My Monitored Chats\n\n"
        "Use the buttons below or the commands:\n"
        "/login /logout /monitoradd /monitortasks /getallid\n"
    )

    keyboard = []
    if is_logged_in:
        keyboard.append([InlineKeyboardButton("📋 My Monitored Chats", callback_data="show_tasks")])
        keyboard.append([InlineKeyboardButton("🔴 Disconnect", callback_data="logout")])
    else:
        keyboard.append([InlineKeyboardButton("🟢 Connect Account", callback_data="login")])

    await update.message.reply_markdown(text, reply_markup=InlineKeyboardMarkup(keyboard))


# ---------- Callback router (single handler for all buttons) ----------
async def callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query:
        return
    # route after verifying authorization (friendly)
    if not await check_authorization(update, context):
        return

    data = query.data or ""
    await query.answer()  # acknowledge early to avoid "button circle"

    # login/logout/buttons navigation and task management
    try:
        if data == "login":
            # start login flow
            try:
                await query.message.delete()
            except Exception:
                pass
            await login_command(update, context)
            return

        if data == "logout":
            try:
                await query.message.delete()
            except Exception:
                pass
            await logout_command(update, context)
            return

        if data == "show_tasks":
            try:
                await query.message.delete()
            except Exception:
                pass
            await monitortasks_command(update, context)
            return

        # chat id categories: chatids_<category>_<page> or chatids_back
        if data.startswith("chatids_"):
            parts = data.split("_")
            # format: chatids_{category}_{page}
            if len(parts) >= 3 and parts[1] != "back":
                category = parts[1]
                try:
                    page = int(parts[2])
                except Exception:
                    page = 0
                await show_categorized_chats(query.from_user.id, query.message.chat.id, query.message.message_id, category, page, context)
            elif data == "chatids_back":
                await show_chat_categories(query.from_user.id, query.message.chat.id, query.message.message_id, context)
            return

        # task_{label}
        if data.startswith("task_"):
            label = data.replace("task_", "", 1)
            await handle_task_menu_by_label(query, context, label)
            return

        if data.startswith("delete_"):
            label = data.replace("delete_", "", 1)
            await confirm_delete_menu(query, context, label)
            return

        if data.startswith("confirm_delete_"):
            label = data.replace("confirm_delete_", "", 1)
            await perform_task_delete(query, context, label)
            return

        # reply payload
        if data.startswith("reply_"):
            payload_b64 = data.replace("reply_", "", 1)
            payload = json_decode_reply_payload(payload_b64)
            if not payload:
                await query.answer("Invalid or expired payload", show_alert=True)
                return
            owner_id = query.from_user.id
            reply_waiting[owner_id] = {
                "user_id": payload.get("u"),
                "chat_id": payload.get("c"),
                "message_id": payload.get("m"),
            }
            await query.message.reply_text(
                "✍️ Please type your reply now. It will be posted to the monitored chat as a reply to the duplicate message."
            )
            return

        # noop (placeholder)
        if data == "noop":
            await query.answer("Not implemented", show_alert=True)
            return

        # fallback: unknown action
        await query.answer("Action not recognized", show_alert=True)

    except Exception:
        logger.exception("Error routing callback: %s", data)
        try:
            await query.answer("An error occurred", show_alert=True)
        except Exception:
            pass


# ---------- Task creation / management ----------
async def monitoradd_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not await check_authorization(update, context):
        return

    user = await db_call(db.get_user, user_id)
    if not user or not user["is_logged_in"]:
        await update.message.reply_text("Please connect your Telegram account first with /login.")
        return

    task_creation_states[user_id] = {"step": "waiting_name", "name": "", "chat_ids": []}
    await update.message.reply_text("Great — what's a friendly name for this monitor task? (e.g. Group Duplicate Checker)")


async def handle_task_creation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in task_creation_states:
        return

    text = (update.message.text or "").strip()
    state = task_creation_states[user_id]

    if state["step"] == "waiting_name":
        if not text:
            await update.message.reply_text("Please enter a non-empty task name.")
            return
        state["name"] = text
        state["step"] = "waiting_chats"
        await update.message.reply_text(
            "Now send the chat ID(s) to monitor, separated by spaces.\n"
            "Example: `-1001234567890 -100987654321`"
        )
        return

    if state["step"] == "waiting_chats":
        parts = [p.strip() for p in text.split() if p.strip()]
        chat_ids = []
        for p in parts:
            try:
                chat_ids.append(int(p))
            except Exception:
                pass
        if not chat_ids:
            await update.message.reply_text("Please provide at least one valid numeric chat ID.")
            return

        settings = {
            "duplicate_detection": True,
            "notify": True,
            "manual_reply": True,
            "auto_reply": False,
            "outgoing": True
        }
        added = await db_call(db.add_monitoring_task, user_id, state["name"], chat_ids, settings)
        if added:
            monitoring_cache.setdefault(user_id, [])
            monitoring_cache[user_id].append({
                "label": state["name"],
                "chat_ids": chat_ids,
                "settings": settings,
                "is_active": 1,
                "user_id": user_id
            })
            await update.message.reply_text(f"✅ Monitoring task *{state['name']}* created — I'll watch those chats for duplicates.", parse_mode="Markdown")
            del task_creation_states[user_id]
        else:
            await update.message.reply_text("A task with that name already exists. Try a different name.")


async def monitortasks_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id if update.effective_user else update.callback_query.from_user.id
    if not await check_authorization(update, context):
        return

    tasks = monitoring_cache.get(user_id) or []
    if not tasks:
        await (update.message.reply_text if update.message else update.callback_query.message.reply_text)(
            "You don't have any monitoring tasks yet. Create one with /monitoradd."
        )
        return

    text = "📋 *Your Monitoring Tasks*\n\n"
    keyboard = []
    for i, t in enumerate(tasks, 1):
        text += f"{i}. *{t['label']}*\n   Chats: `{', '.join(map(str, t['chat_ids']))}`\n\n"
        keyboard.append([InlineKeyboardButton(f"{i}. {t['label']}", callback_data=f"task_{t['label']}")])

    await (update.message.reply_text if update.message else update.callback_query.message.reply_text)(
        text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown"
    )


async def handle_task_menu_by_label(query, context: ContextTypes.DEFAULT_TYPE, label: str):
    user_id = query.from_user.id
    tasks = monitoring_cache.get(user_id, [])
    task = None
    for t in tasks:
        if t["label"] == label:
            task = t
            break
    if not task:
        await query.answer("Task not found", show_alert=True)
        return

    settings = task.get("settings", {})
    outgoing_emoji = "✅" if settings.get("outgoing", True) else "❌"
    duplicate_emoji = "✅" if settings.get("duplicate_detection", True) else "❌"
    manual_emoji = "✅" if settings.get("manual_reply", True) else "❌"

    text = (
        f"🔧 *Task:* {label}\n\n"
        f"Chats: `{', '.join(map(str, task.get('chat_ids', [])))}`\n\n"
        "Settings:\n"
        f"{outgoing_emoji} Outgoing forwarding\n"
        f"{duplicate_emoji} Duplicate detection\n"
        f"{manual_emoji} Manual reply enabled\n\n"
        "Tap an action below:"
    )

    keyboard = [
        [InlineKeyboardButton("✏️ Rename (not implemented)", callback_data="noop")],
        [InlineKeyboardButton("🗑️ Delete task", callback_data=f"delete_{label}")],
        [InlineKeyboardButton("🔙 Back", callback_data="show_tasks")]
    ]

    try:
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
    except Exception:
        # fallback: send a message
        await query.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")


async def confirm_delete_menu(query, context: ContextTypes.DEFAULT_TYPE, label: str):
    text = f"🗑️ *Delete Task:* {label}\n\nAre you sure you want to delete this monitoring task? This cannot be undone."
    keyboard = [
        [
            InlineKeyboardButton("✅ Yes, Delete", callback_data=f"confirm_delete_{label}"),
            InlineKeyboardButton("❌ Cancel", callback_data=f"task_{label}")
        ]
    ]
    try:
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
    except Exception:
        await query.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")


async def perform_task_delete(query, context: ContextTypes.DEFAULT_TYPE, label: str):
    user_id = query.from_user.id
    removed = await db_call(db.remove_monitoring_task, user_id, label)
    if removed:
        # update in-memory cache
        if user_id in monitoring_cache:
            monitoring_cache[user_id] = [t for t in monitoring_cache[user_id] if t.get("label") != label]
        await query.edit_message_text(f"✅ Task *{label}* deleted.", parse_mode="Markdown")
    else:
        await query.edit_message_text(f"❌ Task *{label}* not found.", parse_mode="Markdown")


# ---------- Add/Remove/List allowed users (admin only) ----------
async def adduser_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    caller = update.effective_user.id
    if not await check_authorization(update, context):
        return
    is_admin = await db_call(db.is_user_admin, caller)
    if not is_admin:
        await update.message.reply_text("❌ Only owners/admins can add users.")
        return

    parts = (update.message.text or "").split()
    if len(parts) < 2:
        await update.message.reply_text("Usage: /adduser <USER_ID> [admin]\nExample: /adduser 123456 admin")
        return
    try:
        new_user_id = int(parts[1])
    except Exception:
        await update.message.reply_text("Invalid user id. It must be numeric.")
        return
    is_admin_flag = len(parts) > 2 and parts[2].lower() == "admin"

    added = await db_call(db.add_allowed_user, new_user_id, None, is_admin_flag, caller)
    if added:
        role = "admin" if is_admin_flag else "user"
        await update.message.reply_text(f"✅ Added `{new_user_id}` as {role}.", parse_mode="Markdown")
        try:
            await context.bot.send_message(new_user_id, "✅ You were added. Send /start to begin using the monitor.")
        except Exception:
            logger.exception("Could not notify new user")
    else:
        await update.message.reply_text("❌ That user already exists in allowed list.")


async def removeuser_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    caller = update.effective_user.id
    if not await check_authorization(update, context):
        return
    is_admin = await db_call(db.is_user_admin, caller)
    if not is_admin:
        await update.message.reply_text("❌ Only owners/admins can remove users.")
        return

    parts = (update.message.text or "").split()
    if len(parts) < 2:
        await update.message.reply_text("Usage: /removeuser <USER_ID>\nExample: /removeuser 123456")
        return
    try:
        rem_id = int(parts[1])
    except Exception:
        await update.message.reply_text("Invalid user id. It must be numeric.")
        return

    removed = await db_call(db.remove_allowed_user, rem_id)
    if removed:
        # attempt to disconnect their session if connected
        if rem_id in user_clients:
            try:
                client = user_clients[rem_id]
                handler = handler_registered.get(rem_id)
                if handler:
                    try:
                        client.remove_event_handler(handler)
                    except Exception:
                        pass
                    handler_registered.pop(rem_id, None)
                await client.disconnect()
            except Exception:
                logger.exception("Error disconnecting removed user's client")
            user_clients.pop(rem_id, None)

        await db_call(db.save_user, rem_id, None, None, None, False)
        monitoring_cache.pop(rem_id, None)
        await update.message.reply_text(f"✅ Removed user `{rem_id}`.", parse_mode="Markdown")
        try:
            await context.bot.send_message(rem_id, "❌ You have been removed from access. Contact the owner to be re-added.")
        except Exception:
            logger.exception("Failed to notify removed user")
    else:
        await update.message.reply_text("❌ That user was not found in allowed list.")


async def listusers_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    caller = update.effective_user.id
    if not await check_authorization(update, context):
        return
    is_admin = await db_call(db.is_user_admin, caller)
    if not is_admin:
        await update.message.reply_text("❌ Only owners/admins can list users.")
        return

    users = await db_call(db.get_all_allowed_users)
    if not users:
        await update.message.reply_text("No allowed users found.")
        return

    text = "👥 *Allowed Users*\n\n"
    for u in users:
        role = "👑 Admin" if u["is_admin"] else "👤 User"
        uname = u["username"] or "Unknown"
        text += f"{role} — `{u['user_id']}` ({uname})\n"
    await update.message.reply_text(text, parse_mode="Markdown")


# ---------- Chat listing functions ----------
async def getallid_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Public command for users to fetch their chat IDs (categorised).
    This was missing previously and caused a NameError — now implemented.
    """
    user_id = update.effective_user.id
    if not await check_authorization(update, context):
        return
    user = await db_call(db.get_user, user_id)
    if not user or not user["is_logged_in"]:
        await update.message.reply_text("Please connect your Telegram account first with /login.")
        return
    await update.message.reply_text("🔄 Fetching your chats... Please wait a moment.")
    await show_chat_categories(user_id, update.message.chat.id, None, context)


async def show_chat_categories(user_id: int, chat_id: int, message_id: int, context: ContextTypes.DEFAULT_TYPE):
    if user_id not in user_clients:
        await context.bot.send_message(chat_id, "Please connect your Telegram account with /login to fetch chat IDs.")
        return
    message_text = (
        "🗂️ *Chat Categories*\n\nChoose a category to view chat IDs (10 per page):"
    )
    keyboard = [
        [InlineKeyboardButton("🤖 Bots", callback_data="chatids_bots_0"), InlineKeyboardButton("📢 Channels", callback_data="chatids_channels_0")],
        [InlineKeyboardButton("👥 Groups", callback_data="chatids_groups_0"), InlineKeyboardButton("👤 Private", callback_data="chatids_private_0")],
    ]
    if message_id:
        await context.bot.edit_message_text(message_text, chat_id=chat_id, message_id=message_id, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
    else:
        await context.bot.send_message(chat_id, message_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")


async def show_categorized_chats(user_id: int, chat_id: int, message_id: int, category: str, page: int, context: ContextTypes.DEFAULT_TYPE):
    from telethon.tl.types import User, Channel, Chat
    if user_id not in user_clients:
        return
    client = user_clients[user_id]
    categorized_dialogs = []
    try:
        async for dialog in client.iter_dialogs():
            entity = dialog.entity
            if category == "bots":
                if isinstance(entity, User) and getattr(entity, "bot", False):
                    categorized_dialogs.append(dialog)
            elif category == "channels":
                if isinstance(entity, Channel) and getattr(entity, "broadcast", False):
                    categorized_dialogs.append(dialog)
            elif category == "groups":
                if isinstance(entity, (Channel, Chat)) and not (isinstance(entity, Channel) and getattr(entity, "broadcast", False)):
                    categorized_dialogs.append(dialog)
            elif category == "private":
                if isinstance(entity, User) and not getattr(entity, "bot", False):
                    categorized_dialogs.append(dialog)
    except Exception:
        logger.exception("Error iterating dialogs")
        categorized_dialogs = []

    PAGE_SIZE = 10
    total_pages = max(1, (len(categorized_dialogs) + PAGE_SIZE - 1) // PAGE_SIZE)
    page = max(0, min(page, total_pages - 1))
    start = page * PAGE_SIZE
    end = start + PAGE_SIZE
    page_dialogs = categorized_dialogs[start:end]

    emoji_map = {"bots": "🤖", "channels": "📢", "groups": "👥", "private": "👤"}
    name_map = {"bots": "Bots", "channels": "Channels", "groups": "Groups", "private": "Private"}

    if not categorized_dialogs:
        chat_list = f"{emoji_map.get(category, '💬')} *{name_map.get(category, 'Chats')}*\n\n_No results found._"
    else:
        chat_list = f"{emoji_map.get(category, '💬')} *{name_map.get(category)}* (Page {page+1}/{total_pages})\n\n"
        for i, dialog in enumerate(page_dialogs, start + 1):
            chat_name = dialog.name or "Unknown"
            chat_list += f"{i}. *{chat_name[:40]}*\n   🆔 `{dialog.id}`\n\n"

    keyboard = []
    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"chatids_{category}_{page - 1}"))
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton("Next ➡️", callback_data=f"chatids_{category}_{page + 1}"))
    if nav_row:
        keyboard.append(nav_row)
    keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="chatids_back")])

    try:
        await context.bot.edit_message_text(chat_list, chat_id=chat_id, message_id=message_id, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
    except Exception:
        # send a new message if edit fails
        await context.bot.send_message(chat_id, chat_list, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")


# ---------- Monitoring core (message handler) ----------
def json_encode_reply_payload(user_id:int, chat_id:int, message_id:int) -> str:
    payload = {"u": user_id, "c": chat_id, "m": message_id}
    b = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode().rstrip("=")
    return b

def json_decode_reply_payload(b64:str) -> Optional[Dict]:
    try:
        pad = "=" * (-len(b64) % 4)
        j = base64.urlsafe_b64decode((b64 + pad).encode()).decode()
        return json.loads(j)
    except Exception:
        return None


async def notify_owner(owner_id:int, text:str, keyboard:InlineKeyboardMarkup):
    global APP_INSTANCE
    if APP_INSTANCE:
        try:
            await APP_INSTANCE.bot.send_message(owner_id, text, reply_markup=keyboard, parse_mode="Markdown", disable_web_page_preview=True)
        except Exception:
            logger.exception("Failed to send notification to owner %s", owner_id)
    else:
        logger.warning("APP_INSTANCE not available; cannot notify owner %s", owner_id)


def ensure_handler_registered_for_user(user_id: int, client: TelegramClient):
    if handler_registered.get(user_id):
        return

    async def _message_handler(event):
        try:
            message = getattr(event, "message", None)
            if not message:
                return
            message_text = getattr(event, "raw_text", None) or getattr(message, "message", None) or ""
            if not message_text or not message_text.strip():
                return
            chat_id = getattr(event, "chat_id", None) or getattr(message, "chat_id", None)
            if chat_id is None:
                return

            # ensure monitoring tasks are loaded
            tasks = monitoring_cache.get(user_id)
            if tasks is None:
                try:
                    tasks = await asyncio.to_thread(db.get_user_monitoring_tasks, user_id)
                    monitoring_cache[user_id] = tasks
                except Exception:
                    tasks = []

            for task in tasks:
                if task.get("is_active") != 1:
                    continue
                if chat_id in task.get("chat_ids", []):
                    settings = task.get("settings", {})
                    if settings.get("duplicate_detection", True):
                        dup = await asyncio.to_thread(db.find_duplicate, chat_id, message_text)
                        if dup:
                            # notify owner (task.user_id)
                            notify_text = (
                                f"⚠️ Duplicate detected in chat `{chat_id}` for *{task['label']}*\n\n"
                                f"Preview:\n{message_text[:300]}\n\n"
                                f"Original id: `{dup['message_id']}` seen_at: {dup['seen_at']}"
                            )
                            btn_data = json_encode_reply_payload(user_id, chat_id, dup['message_id'])
                            keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("✉️ Reply", callback_data=f"reply_{btn_data}")]])
                            try:
                                await notify_owner(task.get("user_id", user_id), notify_text, keyboard)
                            except Exception:
                                logger.exception("Failed to notify owner for duplicate")
                        else:
                            # record first-seen message
                            await asyncio.to_thread(db.record_message, chat_id, getattr(message, "id", 0), message_text)
        except Exception:
            logger.exception("Error in monitoring handler for user %s", user_id)

    try:
        client.add_event_handler(_message_handler, events.NewMessage())
        client.add_event_handler(_message_handler, events.MessageEdited())
        handler_registered[user_id] = _message_handler
        logger.info("Registered monitoring handler for user %s", user_id)
    except Exception:
        logger.exception("Failed to register handler for user %s", user_id)


# ---------- Manual reply flow ----------
async def handle_manual_reply_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    owner_id = update.effective_user.id
    # check for logout confirmation or login flow first
    if owner_id in login_states and "logout_confirm" in login_states[owner_id]:
        await handle_logout_confirmation(update, context)
        return
    if owner_id in task_creation_states:
        await handle_task_creation(update, context)
        return
    if owner_id in login_states and login_states[owner_id].get("step"):
        await handle_login_process(update, context)
        return

    if owner_id not in reply_waiting:
        return

    data = reply_waiting.pop(owner_id)
    owner_reply_text = (update.message.text or "").strip()
    if not owner_reply_text:
        await update.message.reply_text("Empty reply — cancelled.")
        return

    job = ("manual_reply", owner_id, data["user_id"], data["chat_id"], data["message_id"], owner_reply_text)
    try:
        await send_queue.put(job)
        await update.message.reply_text("✅ Your reply is queued and will be posted shortly.")
    except Exception:
        logger.exception("Failed to enqueue manual reply job")
        await update.message.reply_text("❌ Failed to enqueue reply. Try again later.")


# ---------- Send worker ----------
async def send_worker_loop(worker_id: int):
    logger.info("Send worker %d started", worker_id)
    global send_queue
    if send_queue is None:
        logger.error("send_worker_loop started before send_queue initialized")
        return
    while True:
        try:
            job = await send_queue.get()
        except asyncio.CancelledError:
            break
        except Exception:
            logger.exception("Error getting item from send_queue")
            await asyncio.sleep(0.5)
            continue
        try:
            if not job:
                continue
            kind = job[0]
            if kind == "manual_reply":
                _, owner_id, user_id, chat_id, message_id, reply_text = job
                client = user_clients.get(user_id)
                if not client:
                    logger.warning("Target user client not connected (%s). Cannot send manual reply.", user_id)
                    try:
                        await APP_INSTANCE.bot.send_message(owner_id, "❌ Target account is not connected. The reply could not be sent.")
                    except Exception:
                        pass
                    continue
                try:
                    target_entity = await client.get_input_entity(int(chat_id))
                    await client.send_message(entity=target_entity, message=reply_text, reply_to=message_id)
                    try:
                        await APP_INSTANCE.bot.send_message(owner_id, "✅ Your reply was posted successfully.")
                    except Exception:
                        logger.exception("Failed to notify owner about sent reply")
                except FloodWaitError as fwe:
                    wait = int(getattr(fwe, "seconds", 10))
                    logger.warning("FloodWait for %s seconds while sending manual reply; requeueing", wait)
                    await asyncio.sleep(wait + 1)
                    try:
                        await send_queue.put(job)
                    except Exception:
                        logger.exception("Failed to requeue manual reply job")
                except Exception:
                    logger.exception("Error sending manual reply job")
            else:
                logger.warning("Unknown job type in queue: %s", kind)
        except Exception:
            logger.exception("Unexpected error in send worker")
        finally:
            try:
                send_queue.task_done()
            except Exception:
                pass


async def start_send_workers():
    global _send_workers_started, send_queue, worker_tasks
    if _send_workers_started:
        return
    if send_queue is None:
        send_queue = asyncio.Queue(maxsize=SEND_QUEUE_MAXSIZE)
    for i in range(SEND_WORKER_COUNT):
        t = asyncio.create_task(send_worker_loop(i + 1))
        worker_tasks.append(t)
    _send_workers_started = True
    logger.info("Spawned %d send workers", SEND_WORKER_COUNT)


# ---------- Session restore & shutdown ----------
async def restore_sessions():
    logger.info("Restoring sessions...")
    try:
        users = await asyncio.to_thread(lambda: db.get_logged_in_users(MAX_CONCURRENT_USERS))
    except Exception:
        logger.exception("Error fetching logged-in users")
        users = []

    for row in users:
        try:
            user_id = row.get("user_id") if isinstance(row, dict) else row[0]
            session_data = row.get("session_data") if isinstance(row, dict) else row[1]
        except Exception:
            continue
        if not session_data:
            continue
        await restore_single_session(user_id, session_data)


async def restore_single_session(user_id: int, session_data: str):
    try:
        client = TelegramClient(StringSession(session_data), API_ID, API_HASH)
        await client.connect()
        if await client.is_user_authorized():
            if len(user_clients) >= MAX_CONCURRENT_USERS:
                logger.info("Skipping restore for %s due to capacity", user_id)
                try:
                    await client.disconnect()
                except Exception:
                    pass
                return
            user_clients[user_id] = client
            monitoring_cache.setdefault(user_id, [])
            ensure_handler_registered_for_user(user_id, client)
            logger.info("Restored session for user %s", user_id)
        else:
            await db_call(db.save_user, user_id, None, None, None, False)
            logger.warning("Session expired for user %s", user_id)
    except Exception:
        logger.exception("Failed to restore session for user %s", user_id)
        try:
            await db_call(db.save_user, user_id, None, None, None, False)
        except Exception:
            pass


async def shutdown_cleanup():
    logger.info("Shutdown: cancelling workers and disconnecting clients...")
    for t in list(worker_tasks):
        try:
            t.cancel()
        except Exception:
            logger.exception("Error cancelling worker")
    if worker_tasks:
        try:
            await asyncio.gather(*worker_tasks, return_exceptions=True)
        except Exception:
            logger.exception("Error awaiting workers")
    # disconnect clients
    for uid, client in list(user_clients.items()):
        handler = handler_registered.get(uid)
        if handler:
            try:
                client.remove_event_handler(handler)
            except Exception:
                logger.exception("Error removing handler during shutdown")
            handler_registered.pop(uid, None)
        try:
            await client.disconnect()
        except Exception:
            logger.exception("Error disconnecting client %s", uid)
    user_clients.clear()
    monitoring_cache.clear()
    try:
        db.close_connection()
    except Exception:
        logger.exception("Error closing DB")
    logger.info("Shutdown cleanup complete.")


# ---------- Application post_init and main ----------
async def post_init(application):
    global MAIN_LOOP
    MAIN_LOOP = asyncio.get_running_loop()
    logger.info("Initializing controller bot...")
    # start webserver
    start_server_thread()

    # ensure owner ids in DB
    if OWNER_IDS:
        for oid in OWNER_IDS:
            try:
                is_admin = await db_call(db.is_user_admin, oid)
                if not is_admin:
                    await db_call(db.add_allowed_user, oid, None, True, None)
                    logger.info("Added owner/admin from env: %s", oid)
            except Exception:
                logger.exception("Error adding owner %s", oid)

    if ALLOWED_USERS:
        for au in ALLOWED_USERS:
            try:
                await db_call(db.add_allowed_user, au, None, False, None)
                logger.info("Added allowed user from env: %s", au)
            except Exception:
                logger.exception("Error adding allowed user %s", au)

    await start_send_workers()
    await restore_sessions()

    def _collect_metrics():
        try:
            q = None
            try:
                q = send_queue.qsize() if send_queue is not None else None
            except Exception:
                q = None
            return {
                "send_queue_size": q,
                "worker_count": len(worker_tasks),
                "active_user_clients_count": len(user_clients),
                "monitoring_tasks_counts": {uid: len(monitoring_cache.get(uid, [])) for uid in list(monitoring_cache.keys())},
                "memory_mb": _get_memory_usage_mb(),
            }
        except Exception as e:
            return {"error": str(e)}

    def _forward_metrics():
        if MAIN_LOOP is not None:
            try:
                future = asyncio.run_coroutine_threadsafe(asyncio.to_thread(_collect_metrics), MAIN_LOOP)
                return future.result(timeout=1.0)
            except Exception:
                return {"error": "failed to collect metrics"}
        return {"error": "main loop not available"}

    try:
        register_monitoring(_forward_metrics)
    except Exception:
        logger.exception("Failed to register monitoring")

    logger.info("Controller bot initialized.")


def _get_memory_usage_mb():
    try:
        import psutil
        process = psutil.Process()
        return round(process.memory_info().rss / 1024 / 1024, 2)
    except Exception:
        return None


def main():
    global APP_INSTANCE
    if not BOT_TOKEN:
        logger.error("BOT_TOKEN not set; aborting")
        return
    if not API_ID or not API_HASH:
        logger.error("API_ID/API_HASH not set; aborting")
        return

    application = Application.builder().token(BOT_TOKEN).post_init(post_init).build()

    # command handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("login", lambda u, c: login_command(u, c)))
    application.add_handler(CommandHandler("logout", lambda u, c: logout_command(u, c)))
    application.add_handler(CommandHandler("monitoradd", monitoradd_command))
    application.add_handler(CommandHandler("monitortasks", monitortasks_command))
    application.add_handler(CommandHandler("getallid", getallid_command))
    application.add_handler(CommandHandler("adduser", adduser_command))
    application.add_handler(CommandHandler("removeuser", removeuser_command))
    application.add_handler(CommandHandler("listusers", listusers_command))

    # single callback handler to route all inline button presses
    application.add_handler(CallbackQueryHandler(callback_router))

    # message handlers: manual reply input and flows
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_manual_reply_input))
    # Also allow login flows to process verification SMS / 2FA
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_login_process))

    APP_INSTANCE = application

    # initialize send workers queue before starting the app
    try:
        asyncio.get_event_loop().run_until_complete(start_send_workers())
    except Exception:
        pass

    logger.info("Starting controller bot (long-polling)...")
    try:
        application.run_polling(drop_pending_updates=True)
    finally:
        # fallback cleanup
        loop_to_use = None
        try:
            if MAIN_LOOP is not None and getattr(MAIN_LOOP, "is_running", lambda: False)():
                loop_to_use = MAIN_LOOP
            else:
                try:
                    running_loop = asyncio.get_running_loop()
                    if getattr(running_loop, "is_running", lambda: False)():
                        loop_to_use = running_loop
                except RuntimeError:
                    loop_to_use = None
        except Exception:
            loop_to_use = None

        if loop_to_use:
            try:
                future = asyncio.run_coroutine_threadsafe(shutdown_cleanup(), loop_to_use)
                future.result(timeout=30)
            except Exception:
                logger.exception("Error waiting for fallback shutdown_cleanup scheduled on running loop")
        else:
            tmp_loop = asyncio.new_event_loop()
            try:
                asyncio.set_event_loop(tmp_loop)
                tmp_loop.run_until_complete(shutdown_cleanup())
            finally:
                try:
                    tmp_loop.close()
                except Exception:
                    pass
                try:
                    asyncio.set_event_loop(None)
                except Exception:
                    pass


# ---- Login helpers (kept near end to avoid forward refs issues) ----
async def login_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id if update.effective_user else (update.callback_query.from_user.id if update.callback_query else None)
    if user_id is None:
        return
    if not await check_authorization(update, context):
        return
    msg = update.message if update.message else update.callback_query.message
    if len(user_clients) >= MAX_CONCURRENT_USERS:
        await msg.reply_text("Server is at capacity. Please try again later.")
        return
    user = await db_call(db.get_user, user_id)
    if user and user.get("is_logged_in"):
        await msg.reply_text("You're already connected. Use /logout to disconnect.")
        return
    client = TelegramClient(StringSession(), API_ID, API_HASH)
    try:
        await client.connect()
    except Exception as e:
        logger.exception("Telethon connect failed: %s", e)
        await msg.reply_text("Failed to connect to Telegram. Try again later.")
        return
    login_states[user_id] = {"client": client, "step": "waiting_phone"}
    await msg.reply_text("Please send your phone number in international format (e.g. +1234567890).")


async def handle_login_process(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    # if this is a manual reply flow, will be handled elsewhere
    if user_id not in login_states:
        return
    state = login_states[user_id]
    text = (update.message.text or "").strip()
    client = state["client"]
    try:
        if state["step"] == "waiting_phone":
            if not text.startswith("+"):
                await update.message.reply_text("Phone must start with +. Try again.")
                return
            clean_phone = ''.join(c for c in text if c.isdigit() or c == '+')
            try:
                result = await client.send_code_request(clean_phone)
                state["phone"] = clean_phone
                state["phone_code_hash"] = getattr(result, "phone_code_hash", None)
                state["step"] = "waiting_code"
                await update.message.reply_text("Code sent. Reply with `verify12345` (e.g. `verify54321`).", parse_mode="Markdown")
            except Exception as e:
                logger.exception("send_code_request failed")
                await update.message.reply_text(f"Failed to send code: {e}")
                try:
                    await client.disconnect()
                except Exception:
                    pass
                if user_id in login_states:
                    del login_states[user_id]
        elif state["step"] == "waiting_code":
            if not text.startswith("verify"):
                await update.message.reply_text("Use `verify12345` format.")
                return
            code = text[6:]
            try:
                await client.sign_in(state["phone"], code, phone_code_hash=state.get("phone_code_hash"))
                me = await client.get_me()
                session_string = client.session.save()
                await db_call(db.save_user, user_id, state["phone"], me.first_name, session_string, True)
                user_clients[user_id] = client
                monitoring_cache.setdefault(user_id, [])
                ensure_handler_registered_for_user(user_id, client)
                del login_states[user_id]
                await update.message.reply_text("You're connected! 🎉 You can now create monitors with /monitoradd.")
            except SessionPasswordNeededError:
                state["step"] = "waiting_2fa"
                await update.message.reply_text("This account has 2FA. Send password as `passwordYourPassword`.", parse_mode="Markdown")
            except Exception as e:
                logger.exception("Error signing in: %s", e)
                await update.message.reply_text(f"Sign-in failed: {e}")
                try:
                    await client.disconnect()
                except Exception:
                    pass
                if user_id in login_states:
                    del login_states[user_id]
        elif state["step"] == "waiting_2fa":
            if not text.startswith("password"):
                await update.message.reply_text("Use `passwordYourPassword` format.")
                return
            password = text[8:]
            try:
                await client.sign_in(password=password)
                me = await client.get_me()
                session_string = client.session.save()
                await db_call(db.save_user, user_id, state.get("phone"), me.first_name, session_string, True)
                user_clients[user_id] = client
                monitoring_cache.setdefault(user_id, [])
                ensure_handler_registered_for_user(user_id, client)
                del login_states[user_id]
                await update.message.reply_text("Connected with 2FA. 🎉")
            except Exception as e:
                logger.exception("2FA sign in failed")
                await update.message.reply_text(f"2FA failed: {e}")
    except Exception:
        logger.exception("Unexpected error during login process")
        try:
            await client.disconnect()
        except Exception:
            pass
        if user_id in login_states:
            del login_states[user_id]
        await update.message.reply_text("Unexpected error. Please try /login again.")


# ---------- Logout confirmation (handled in message flow) ----------
async def logout_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id if update.effective_user else (update.callback_query.from_user.id if update.callback_query else None)
    if user_id is None:
        return
    if not await check_authorization(update, context):
        return
    user = await db_call(db.get_user, user_id)
    if not user or not user["is_logged_in"]:
        await (update.message.reply_text if update.message else update.callback_query.message.reply_text)("You're not connected.")
        return
    login_states[user_id] = {"logout_confirm": user["phone"]}
    await (update.message.reply_text if update.message else update.callback_query.message.reply_text)(
        f"To confirm logout, please type your phone number exactly as shown: `{user['phone']}`", parse_mode="Markdown"
    )


async def handle_logout_confirmation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    user_id = update.effective_user.id
    if user_id not in login_states or "logout_confirm" not in login_states[user_id]:
        return False
    expected = login_states[user_id]["logout_confirm"]
    if update.message.text.strip() != expected:
        await update.message.reply_text("Phone doesn't match. Logout cancelled.")
        del login_states[user_id]
        return True
    if user_id in user_clients:
        client = user_clients[user_id]
        try:
            handler = handler_registered.get(user_id)
            if handler:
                try:
                    client.remove_event_handler(handler)
                except Exception:
                    pass
                handler_registered.pop(user_id, None)
            await client.disconnect()
        except Exception:
            logger.exception("Error disconnecting client")
        user_clients.pop(user_id, None)
    await db_call(db.save_user, user_id, None, None, None, False)
    monitoring_cache.pop(user_id, None)
    del login_states[user_id]
    await update.message.reply_text("You're disconnected. 👋")
    return True


if __name__ == "__main__":
    main()
