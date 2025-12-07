#!/usr/bin/env python3
"""
Duplicate Message Monitor + Manual Reply System

Single-file entrypoint for Render deployment. It uses:
 - Telethon to manage user sessions (multiple user accounts)
 - python-telegram-bot for the controller bot (commands, UI)
 - webserver.py for health & metrics
 - database.py for persistent storage
"""

import os
import asyncio
import logging
import functools
import re
import time
import signal
import threading
from typing import Dict, List, Optional, Tuple, Any, Set
from collections import OrderedDict
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

# Environment variables
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
reply_waiting: Dict[int, Dict] = {}  # notify_message_id -> {owner_id, user_id, chat_id, message_id, preview}

# global send queue (used to send replies into chats)
send_queue: Optional[asyncio.Queue] = None
worker_tasks: List[asyncio.Task] = []
_send_workers_started = False
MAIN_LOOP: Optional[asyncio.AbstractEventLoop] = None

UNAUTHORIZED_MESSAGE = """🚫 **Access Denied!** 

You are not authorized to use this system.

📞 **Call this number:** `07089430305`

Or

🗨️ **Message Developer:** [HEMMY](https://t.me/justmemmy)
"""

# small helpers to run DB calls in thread
async def db_call(func, *args, **kwargs):
    return await asyncio.to_thread(functools.partial(func, *args, **kwargs))


# Authorization check
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
            await update.message.reply_text(UNAUTHORIZED_MESSAGE, parse_mode="Markdown", disable_web_page_preview=True)
        elif update.callback_query:
            await update.callback_query.answer()
            await update.callback_query.message.reply_text(UNAUTHORIZED_MESSAGE, parse_mode="Markdown", disable_web_page_preview=True)
        return False
    return True

# ---- Minimal UI commands (start/login/logout/monitoradd/monitortasks/getallid/adduser/removeuser/listusers) ----

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not await check_authorization(update, context):
        return
    user = await db_call(db.get_user, user_id)
    user_name = update.effective_user.first_name or "User"
    user_phone = user["phone"] if user and user["phone"] else "Not connected"
    is_logged_in = user and user["is_logged_in"]
    status_emoji = "🟢" if is_logged_in else "🔴"
    status_text = "Online" if is_logged_in else "Offline"
    message_text = f"""
╔═══════════════════════════╗
║   🛡️ DUPLICATE MONITOR 🛡️  ║
╚═══════════════════════════╝

👤 **User:** {user_name}
📱 **Phone:** `{user_phone}`
{status_emoji} **Status:** {status_text}

Commands:
• /login - Connect account
• /logout - Disconnect
• /monitoradd - Add monitor task
• /monitortasks - Manage tasks
• /getallid - Get chat IDs
"""
    keyboard = []
    if is_logged_in:
        keyboard.append([InlineKeyboardButton("📋 My Monitored Chats", callback_data="show_tasks")])
        keyboard.append([InlineKeyboardButton("🔴 Disconnect", callback_data="logout")])
    else:
        keyboard.append([InlineKeyboardButton("🟢 Connect Account", callback_data="login")])
    await update.message.reply_text(message_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not await check_authorization(update, context):
        return
    await query.answer()
    if query.data == "login":
        await query.message.delete()
        await login_command(update, context)
    elif query.data == "logout":
        await query.message.delete()
        await logout_command(update, context)
    elif query.data == "show_tasks":
        await query.message.delete()
        await monitortasks_command(update, context)

# ---- Task creation ----
async def monitoradd_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not await check_authorization(update, context):
        return
    user = await db_call(db.get_user, user_id)
    if not user or not user["is_logged_in"]:
        await update.message.reply_text("❌ Connect your account first with /login", parse_mode="Markdown")
        return
    task_creation_states[user_id] = {"step": "waiting_name", "name": "", "chat_ids": []}
    await update.message.reply_text("📝 Step 1: Enter a name for this monitoring task.", parse_mode="Markdown")

async def handle_task_creation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = update.message.text.strip()
    if user_id not in task_creation_states:
        return
    state = task_creation_states[user_id]
    try:
        if state["step"] == "waiting_name":
            if not text:
                await update.message.reply_text("Please provide a valid name.")
                return
            state["name"] = text
            state["step"] = "waiting_chats"
            await update.message.reply_text("📥 Step 2: Enter chat ID(s) to monitor (space separated). Example: -1001234567890", parse_mode="Markdown")
        elif state["step"] == "waiting_chats":
            parts = [p.strip() for p in text.split() if p.strip()]
            chat_ids = []
            for p in parts:
                try:
                    chat_ids.append(int(p))
                except Exception:
                    pass
            if not chat_ids:
                await update.message.reply_text("Please enter at least one valid numeric chat ID.")
                return
            state["chat_ids"] = chat_ids
            settings = {
                "duplicate_detection": True,
                "notify": True,
                "manual_reply": True,
                "auto_reply": False,
                "outgoing": True
            }
            added = await db_call(db.add_monitoring_task, user_id, state["name"], state["chat_ids"], settings)
            if added:
                monitoring_cache.setdefault(user_id, [])
                monitoring_cache[user_id].append({
                    "label": state["name"],
                    "chat_ids": state["chat_ids"],
                    "settings": settings,
                    "is_active": 1
                })
                # schedule resolving possibly needed in background (not required for monitoring)
                await update.message.reply_text(f"✅ Monitoring task '{state['name']}' created!", parse_mode="Markdown")
                del task_creation_states[user_id]
            else:
                await update.message.reply_text("❌ Task with that name already exists. Choose another.")
    except Exception:
        logger.exception("Error in task creation")
        if user_id in task_creation_states:
            del task_creation_states[user_id]
        await update.message.reply_text("❌ Error creating monitor task. Try again.")

async def monitortasks_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id if update.effective_user else update.callback_query.from_user.id
    if not await check_authorization(update, context):
        return
    message = update.message if update.message else update.callback_query.message
    tasks = monitoring_cache.get(user_id) or []
    if not tasks:
        await message.reply_text("📋 No active monitoring tasks. Use /monitoradd to create one.", parse_mode="Markdown")
        return
    text = "📋 Your Monitoring Tasks\n\n"
    keyboard = []
    for i, t in enumerate(tasks, 1):
        text += f"{i}. **{t['label']}**\n   Chats: {', '.join(map(str, t['chat_ids']))}\n\n"
        keyboard.append([InlineKeyboardButton(f"{i}. {t['label']}", callback_data=f"task_{t['label']}")])
    await message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

# ---- Login / Logout handling (Telethon) ----
async def login_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id if update.effective_user else update.callback_query.from_user.id
    if not await check_authorization(update, context):
        return
    message = update.message if update.message else update.callback_query.message
    if len(user_clients) >= MAX_CONCURRENT_USERS:
        await message.reply_text("❌ Server at capacity. Try later.", parse_mode="Markdown")
        return
    user = await db_call(db.get_user, user_id)
    if user and user.get("is_logged_in"):
        await message.reply_text("✅ You are already logged in.", parse_mode="Markdown")
        return
    client = TelegramClient(StringSession(), API_ID, API_HASH)
    try:
        await client.connect()
    except Exception as e:
        logger.error("Telethon connect failed: %s", e)
        await message.reply_text("❌ Connection failed. Try again later.", parse_mode="Markdown")
        return
    login_states[user_id] = {"client": client, "step": "waiting_phone"}
    await message.reply_text("📱 Enter your phone number (with +):", parse_mode="Markdown")

async def handle_login_process(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    # task creation has precedence
    if user_id in task_creation_states:
        await handle_task_creation(update, context)
        return
    if user_id not in login_states:
        return
    state = login_states[user_id]
    text = update.message.text.strip()
    client = state["client"]
    try:
        if state["step"] == "waiting_phone":
            if not text.startswith("+"):
                await update.message.reply_text("Phone must start with +. Try again.", parse_mode="Markdown")
                return
            clean_phone = ''.join(c for c in text if c.isdigit() or c == '+')
            try:
                result = await client.send_code_request(clean_phone)
                state["phone"] = clean_phone
                state["phone_code_hash"] = result.phone_code_hash
                state["step"] = "waiting_code"
                await update.message.reply_text("✅ Code sent. Reply with verify12345 (e.g. verify54321).", parse_mode="Markdown")
            except Exception as e:
                logger.exception("send_code_request failed")
                await update.message.reply_text(f"❌ Error sending code: {e}", parse_mode="Markdown")
                try:
                    await client.disconnect()
                except Exception:
                    pass
                del login_states[user_id]
        elif state["step"] == "waiting_code":
            if not text.startswith("verify"):
                await update.message.reply_text("Use format verify12345.", parse_mode="Markdown")
                return
            code = text[6:]
            try:
                await client.sign_in(state["phone"], code, phone_code_hash=state.get("phone_code_hash"))
                me = await client.get_me()
                session_string = client.session.save()
                await db_call(db.save_user, user_id, state["phone"], me.first_name, session_string, True)
                user_clients[user_id] = client
                monitoring_cache.setdefault(user_id, [])
                # register handler
                ensure_handler_registered_for_user(user_id, client)
                del login_states[user_id]
                await update.message.reply_text("✅ Successfully connected!", parse_mode="Markdown")
            except SessionPasswordNeededError:
                state["step"] = "waiting_2fa"
                await update.message.reply_text("🔐 This account requires 2FA. Send password as passwordYourPassword123", parse_mode="Markdown")
            except Exception as e:
                logger.exception("Error signing in: %s", e)
                await update.message.reply_text(f"❌ Verification failed: {e}", parse_mode="Markdown")
                try:
                    await client.disconnect()
                except Exception:
                    pass
                if user_id in login_states:
                    del login_states[user_id]
        elif state["step"] == "waiting_2fa":
            if not text.startswith("password"):
                await update.message.reply_text("Use format passwordYourPassword123", parse_mode="Markdown")
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
                await update.message.reply_text("✅ Connected with 2FA!", parse_mode="Markdown")
            except Exception as e:
                logger.exception("2FA sign in failed")
                await update.message.reply_text(f"❌ 2FA verification failed: {e}", parse_mode="Markdown")
    except Exception:
        logger.exception("Unexpected error during login process")
        if user_id in login_states:
            try:
                await login_states[user_id]["client"].disconnect()
            except Exception:
                pass
            del login_states[user_id]
        await update.message.reply_text("❌ Unexpected error. Try /login again.", parse_mode="Markdown")

async def logout_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id if update.effective_user else update.callback_query.from_user.id
    if not await check_authorization(update, context):
        return
    message = update.message if update.message else update.callback_query.message
    user = await db_call(db.get_user, user_id)
    if not user or not user["is_logged_in"]:
        await message.reply_text("❌ You're not connected.", parse_mode="Markdown")
        return
    # require phone confirmation
    await message.reply_text(f"⚠️ Confirm logout by typing your phone number exactly: `{user['phone']}`", parse_mode="Markdown")
    # set a one-shot state
    login_states[user_id] = {"logout_confirm": user["phone"]}

async def handle_logout_confirmation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id not in login_states or "logout_confirm" not in login_states[user_id]:
        return False
    expected = login_states[user_id]["logout_confirm"]
    if update.message.text.strip() != expected:
        await update.message.reply_text("Phone doesn't match. Logout cancelled.", parse_mode="Markdown")
        del login_states[user_id]
        return True
    # proceed to disconnect if connected
    if user_id in user_clients:
        client = user_clients[user_id]
        try:
            handler = handler_registered.get(user_id)
            if handler:
                try:
                    client.remove_event_handler(handler)
                except Exception:
                    logger.exception("Error removing handler")
                handler_registered.pop(user_id, None)
            await client.disconnect()
        except Exception:
            logger.exception("Error disconnecting client")
        user_clients.pop(user_id, None)
    await db_call(db.save_user, user_id, None, None, None, False)
    monitoring_cache.pop(user_id, None)
    del login_states[user_id]
    await update.message.reply_text("👋 Disconnected successfully.", parse_mode="Markdown")
    return True

# ---- Chat listing functions (getallid) ----
async def getallid_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not await check_authorization(update, context):
        return
    user = await db_call(db.get_user, user_id)
    if not user or not user["is_logged_in"]:
        await update.message.reply_text("❌ Connect your account first with /login", parse_mode="Markdown")
        return
    await update.message.reply_text("🔄 Fetching your chats...")
    await show_chat_categories(user_id, update.message.chat.id, None, context)

# show_chat_categories and pagination reuse similar approach to original forward.py
async def show_chat_categories(user_id: int, chat_id: int, message_id: int, context: ContextTypes.DEFAULT_TYPE):
    if user_id not in user_clients:
        return
    message_text = (
        "🗂️ **Chat ID Categories**\n\n"
        "Select category:"
    )
    keyboard = [
        [InlineKeyboardButton("🤖 Bots", callback_data="chatids_bots_0"), InlineKeyboardButton("📢 Channels", callback_data="chatids_channels_0")],
        [InlineKeyboardButton("👥 Groups", callback_data="chatids_groups_0"), InlineKeyboardButton("👤 Private", callback_data="chatids_private_0")],
    ]
    if message_id:
        await context.bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=message_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
    else:
        await context.bot.send_message(chat_id=chat_id, text=message_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

# note: for brevity the paginated listing logic is simple and synchronous in this sample
async def show_categorized_chats(user_id: int, chat_id: int, message_id: int, category: str, page: int, context: ContextTypes.DEFAULT_TYPE):
    from telethon.tl.types import User, Channel, Chat
    if user_id not in user_clients:
        return
    client = user_clients[user_id]
    categorized_dialogs = []
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
    PAGE_SIZE = 10
    total_pages = max(1, (len(categorized_dialogs) + PAGE_SIZE - 1) // PAGE_SIZE)
    start = page * PAGE_SIZE
    end = start + PAGE_SIZE
    page_dialogs = categorized_dialogs[start:end]
    name_map = {"bots": "Bots", "channels": "Channels", "groups": "Groups", "private": "Private"}
    emoji_map = {"bots": "🤖", "channels": "📢", "groups": "👥", "private": "👤"}
    if not categorized_dialogs:
        chat_list = f"{emoji_map.get(category, '💬')} **{name_map.get(category, 'Chats')}**\n\nNo results."
    else:
        chat_list = f"{emoji_map.get(category, '💬')} **{name_map.get(category, 'Chats')} (Page {page+1}/{total_pages})**\n\n"
        for i, dialog in enumerate(page_dialogs, start + 1):
            chat_name = dialog.name[:30] if dialog.name else "Unknown"
            chat_list += f"{i}. **{chat_name}**\n   🆔 `{dialog.id}`\n\n"
    keyboard = []
    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"chatids_{category}_{page - 1}"))
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton("Next ➡️", callback_data=f"chatids_{category}_{page + 1}"))
    if nav_row:
        keyboard.append(nav_row)
    keyboard.append([InlineKeyboardButton("🔙 Back to Categories", callback_data="chatids_back")])
    await context.bot.edit_message_text(chat_list, chat_id=chat_id, message_id=message_id, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")

# ---- Monitoring core ----
def ensure_handler_registered_for_user(user_id: int, client: TelegramClient):
    if handler_registered.get(user_id):
        return

    async def _message_handler(event):
        try:
            message = getattr(event, "message", None) or getattr(event, "original_update", None)
            if not message:
                return
            message_text = getattr(event, "raw_text", None) or getattr(message, "message", None) or ""
            # ignore empty messages
            if not message_text or not message_text.strip():
                return
            chat_id = getattr(event, "chat_id", None) or getattr(message, "chat_id", None)
            if chat_id is None:
                return
            # get user tasks watching this chat
            user_tasks = monitoring_cache.get(user_id, [])  # in-memory cache
            if not user_tasks:
                # lazy load from DB if absent
                try:
                    tasks = await db_call(db.get_user_monitoring_tasks, user_id)
                    monitoring_cache[user_id] = tasks
                    user_tasks = tasks
                except Exception:
                    user_tasks = []
            for task in user_tasks:
                if task.get("is_active") != 1:
                    continue
                if chat_id in task.get("chat_ids", []):
                    settings = task.get("settings", {})
                    # only duplicate detection path
                    if settings.get("duplicate_detection", True):
                        dup = await asyncio.to_thread(db.find_duplicate, chat_id, message_text)
                        if dup:
                            # notify owner(s) - for simplicity notify the owner/allowed user who created the task
                            # prepare a summary and an inline button to "Reply"
                            notify_text = (
                                f"⚠️ Duplicate detected in chat `{chat_id}` for task *{task['label']}*\n\n"
                                f"Message preview:\n{message_text[:300]}\n\n"
                                f"Original message id: `{dup['message_id']}` seen_at: {dup['seen_at']}"
                            )
                            # send the notification to the owner (task owner is task.user_id)
                            try:
                                # we send via controller bot (telegram bot) to the task owner
                                # create an inline button that encodes where to reply to
                                btn_data = json_encode_reply_payload(user_id, chat_id, dup['message_id'], message_text[:200])
                                keyboard = [[InlineKeyboardButton("✉️ Reply", callback_data=f"reply_{btn_data}")]]
                                # send to owner via bot - use python-telegram-bot context via threading to avoid loop conflicts
                                await notify_owner(task['user_id'], notify_text, InlineKeyboardMarkup(keyboard))
                                # record the duplicate occurrence for traceability
                                await asyncio.to_thread(db.record_message, chat_id, getattr(message, "id", dup['message_id']), message_text)
                            except Exception:
                                logger.exception("Failed to notify owner for duplicate")
                        else:
                            # record first-seen message (only when not duplicate)
                            await asyncio.to_thread(db.record_message, chat_id, getattr(message, "id", 0), message_text)
        except Exception:
            logger.exception("Error in message handler for user %s", user_id)

    try:
        client.add_event_handler(_message_handler, events.NewMessage())
        client.add_event_handler(_message_handler, events.MessageEdited())
        handler_registered[user_id] = _message_handler
        logger.info("Registered monitoring handler for user %s", user_id)
    except Exception:
        logger.exception("Failed to register handler for user %s", user_id)

# helper to encode small payload safely in callback_data (base64-like)
import base64
import json

def json_encode_reply_payload(user_id:int, chat_id:int, message_id:int, preview:str) -> str:
    payload = {"u": user_id, "c": chat_id, "m": message_id}
    b = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode()
    # callback_data length is limited by Telegram; keep compact
    return b

def json_decode_reply_payload(b64:str) -> Optional[Dict]:
    try:
        j = base64.urlsafe_b64decode(b64.encode()).decode()
        return json.loads(j)
    except Exception:
        return None

# notify_owner posts a message via the controller bot (telegram bot) to the owner user_id
async def notify_owner(owner_id:int, text:str, keyboard:InlineKeyboardMarkup):
    # Use bot's Application instance to send (we keep it in global variable below)
    global APP_INSTANCE
    if APP_INSTANCE:
        try:
            await APP_INSTANCE.bot.send_message(owner_id, text, reply_markup=keyboard, parse_mode="Markdown", disable_web_page_preview=True)
        except Exception:
            logger.exception("Failed to send notification to owner %s", owner_id)
    else:
        logger.warning("APP_INSTANCE not available; cannot notify owner %s", owner_id)

# ---- Manual reply handling ----
# When owner clicks "Reply" button, open a small flow where owner's next text is captured and bridged to monitored chat
async def button_handler_root(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not await check_authorization(update, context):
        return
    await query.answer()
    if query.data.startswith("reply_"):
        payload_b64 = query.data.replace("reply_", "")
        payload = json_decode_reply_payload(payload_b64)
        if not payload:
            await query.answer("Invalid payload", show_alert=True)
            return
        # store waiting mapping keyed by owner id
        owner_id = query.from_user.id
        reply_waiting[owner_id] = {
            "user_id": payload.get("u"),
            "chat_id": payload.get("c"),
            "message_id": payload.get("m"),
            "task_preview": "You may reply now. Your reply will be posted to the monitored chat and reply to the duplicate message."
        }
        await query.message.reply_text("✍️ Send me your reply now. It will be posted to the monitored chat as a reply to the duplicate message.", parse_mode="Markdown")

async def handle_manual_reply_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    owner_id = update.effective_user.id
    if owner_id not in reply_waiting:
        # may be other flows like task creation or login
        # reuse existing handlers
        if owner_id in login_states and "logout_confirm" in login_states[owner_id]:
            # it's a logout confirmation
            await handle_logout_confirmation(update, context)
            return
        if owner_id in task_creation_states:
            await handle_task_creation(update, context)
            return
        if owner_id in login_states and login_states[owner_id].get("step"):
            await handle_login_process(update, context)
            return
        return
    data = reply_waiting.pop(owner_id)
    owner_reply_text = update.message.text or ""
    # schedule send via send_queue: (owner_id, target_user_id, chat_id, message_id, reply_text)
    job = ("manual_reply", owner_id, data["user_id"], data["chat_id"], data["message_id"], owner_reply_text)
    try:
        await send_queue.put(job)
    except Exception:
        logger.exception("Failed to enqueue manual reply job")
        await update.message.reply_text("❌ Failed to enqueue reply. Try again.", parse_mode="Markdown")

# ---- Worker that dispatches manual replies through Telethon clients ----
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
                    continue
                try:
                    # Resolve input entity and reply to message_id
                    target_entity = await client.get_input_entity(int(chat_id))
                    await client.send_message(entity=target_entity, message=reply_text, reply_to=message_id)
                    # Notify the owner that reply was sent
                    try:
                        await APP_INSTANCE.bot.send_message(owner_id, "✅ Your reply was posted.", parse_mode="Markdown")
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

# ---- Session restore and graceful shutdown ----
async def restore_sessions():
    logger.info("Restoring sessions...")
    try:
        users = await asyncio.to_thread(lambda: db.get_logged_in_users(MAX_CONCURRENT_USERS))
    except Exception:
        logger.exception("Error fetching logged-in users")
        users = []
    monitoring_cache.clear()
    try:
        all_tasks = await db_call(db.get_user_monitoring_tasks, None)  # intentionally may raise; fallback handled below
    except Exception:
        all_tasks = []
    # load monitoring tasks per user from DB
    try:
        # get all tasks by iterating per user recovered sessions only
        for row in users:
            try:
                user_id = row.get("user_id") if isinstance(row, dict) else row[0]
                session_data = row.get("session_data") if isinstance(row, dict) else row[1]
            except Exception:
                continue
            if session_data:
                await restore_single_session(user_id, session_data)
    except Exception:
        logger.exception("Error while restoring sessions")

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

# ---- Application post_init and main ----
async def post_init(application):
    global MAIN_LOOP
    MAIN_LOOP = asyncio.get_running_loop()
    logger.info("Initializing controller bot...")
    # start webserver
    start_server_thread()
    # add owners from env to allowed users
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

    # register a simple metrics callback
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

# minimal global to allow sending messages from monitoring core
APP_INSTANCE = None

def _get_memory_usage_mb():
    try:
        import psutil
        process = psutil.Process()
        return round(process.memory_info().rss / 1024 / 1024, 2)
    except ImportError:
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
    application.add_handler(CommandHandler("login", login_command))
    application.add_handler(CommandHandler("logout", logout_command))
    application.add_handler(CommandHandler("monitoradd", monitoradd_command))
    application.add_handler(CommandHandler("monitortasks", monitortasks_command))
    application.add_handler(CommandHandler("getallid", getallid_command))
    application.add_handler(CallbackQueryHandler(button_handler))
    application.add_handler(CallbackQueryHandler(button_handler_root, pattern=r"^reply_"))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_manual_reply_input))
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
        try:
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

if __name__ == "__main__":
    main()