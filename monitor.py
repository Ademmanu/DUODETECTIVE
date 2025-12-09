#!/usr/bin/env python3
import os
import asyncio
import logging
import functools
import hashlib
import time
import gc
import sys
import json
from typing import Dict, List, Optional, Tuple, Set, Callable, Any
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
from telegram.helpers import escape_markdown
from database import Database
from webserver import start_server_thread, register_monitoring

# Enhanced logging with file output
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('bot_debug.log')
    ]
)
logger = logging.getLogger("monitor")
logger.setLevel(logging.DEBUG)

# Environment variables
BOT_TOKEN = os.getenv("BOT_TOKEN")
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")

# Support multiple owners / admins via OWNER_IDS (comma-separated)
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

# Support additional allowed users via ALLOWED_USERS (comma-separated)
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

# User sessions stored in environment variable (comma-separated key:value pairs)
USER_SESSIONS: Dict[str, str] = {}
user_sessions_env = os.getenv("USER_SESSIONS", "").strip()
if user_sessions_env:
    try:
        # Format: "user_id1:session1,user_id2:session2"
        for pair in user_sessions_env.split(","):
            pair = pair.strip()
            if not pair or ":" not in pair:
                continue
            user_id_str, session = pair.split(":", 1)
            USER_SESSIONS[user_id_str.strip()] = session.strip()
        logger.info(f"Loaded {len(USER_SESSIONS)} user sessions from environment")
    except Exception as e:
        logger.exception(f"Error parsing USER_SESSIONS: {e}")

# Tuning parameters for 50 concurrent users
MONITOR_WORKER_COUNT = int(os.getenv("MONITOR_WORKER_COUNT", "10"))
SEND_QUEUE_MAXSIZE = int(os.getenv("SEND_QUEUE_MAXSIZE", "2000"))
DUPLICATE_CHECK_WINDOW = int(os.getenv("DUPLICATE_CHECK_WINDOW", "600"))  # 600 seconds = 10 minutes
MAX_CONCURRENT_USERS = int(os.getenv("MAX_CONCURRENT_USERS", "50"))
MESSAGE_HASH_LIMIT = int(os.getenv("MESSAGE_HASH_LIMIT", "2000"))

db = Database()

# Global bot instance for notifications
BOT_INSTANCE = None

# Data structures
user_clients: Dict[int, TelegramClient] = {}
login_states: Dict[int, Dict] = {}
logout_states: Dict[int, Dict] = {}
reply_states: Dict[int, Dict] = {}
auto_reply_states: Dict[int, Dict] = {}  # user_id -> {task_label: message}

# Task creation states
task_creation_states: Dict[int, Dict[str, Any]] = {}

# Caches
tasks_cache: Dict[int, List[Dict]] = {}
chat_entity_cache: Dict[int, Dict[int, object]] = {}
handler_registered: Dict[int, List[Callable]] = {}  # user_id -> list of handlers
message_history: Dict[Tuple[int, int], List[Tuple[str, float, str]]] = {}
notification_messages: Dict[int, Dict] = {}  # message_id -> {user_id, task_label, chat_id, original_message_id, duplicate_hash, message_preview}

# Global queues
notification_queue: Optional[asyncio.Queue] = None

UNAUTHORIZED_MESSAGE = """🚫 **Access Denied!** 

You are not authorized to use this system.

📞 **Call this number:** `07089430305`

Or

🗨️ **Message Developer:** [HEMMY](https://t.me/justmemmy)
"""

# Track worker tasks
worker_tasks: List[asyncio.Task] = []
_workers_started = False

# Main loop reference
MAIN_LOOP: Optional[asyncio.AbstractEventLoop] = None

# Memory management
_last_gc_run = 0
GC_INTERVAL = 300


async def db_call(func, *args, **kwargs):
    return await asyncio.to_thread(functools.partial(func, *args, **kwargs))


async def optimized_gc():
    global _last_gc_run
    current_time = asyncio.get_event_loop().time()
    if current_time - _last_gc_run > GC_INTERVAL:
        collected = gc.collect()
        logger.debug(f"Garbage collection freed {collected} objects")
        _last_gc_run = current_time


# ---------- Session Management ----------
async def save_user_session_to_env(user_id: int, session_string: str, phone: str, name: str):
    """Save user session to environment variable format and notify owners"""
    try:
        # Update in-memory sessions
        USER_SESSIONS[str(user_id)] = session_string
        
        # Create environment variable format
        session_pairs = []
        for uid, sess in USER_SESSIONS.items():
            session_pairs.append(f"{uid}:{sess}")
        
        env_format = ",".join(session_pairs)
        
        # Send session to all owners
        for owner_id in OWNER_IDS:
            try:
                await BOT_INSTANCE.send_message(
                    chat_id=owner_id,
                    text=(
                        f"🔑 **New User Session String**\n\n"
                        f"👤 **User ID:** `{user_id}`\n"
                        f"📱 **Phone:** `{phone}`\n"
                        f"👤 **Name:** `{name}`\n\n"
                        f"**Session String:**\n"
                        f"```\n{session_string}\n```\n\n"
                        f"**Add to USER_SESSIONS env var:**\n"
                        f"`USER_SESSIONS={env_format}`\n\n"
                        f"**Or append to existing:**\n"
                        f"`,{user_id}:{session_string}`"
                    ),
                    parse_mode="Markdown"
                )
                logger.info(f"Sent session string for user {user_id} to owner {owner_id}")
            except Exception as e:
                logger.error(f"Failed to send session to owner {owner_id}: {e}")
        
        # Also log the session (for debugging)
        logger.info(f"Session for user {user_id} saved. Total sessions: {len(USER_SESSIONS)}")
        
    except Exception as e:
        logger.exception(f"Error saving user session to env: {e}")


async def remove_user_session_from_env(user_id: int):
    """Remove user session from environment variable format"""
    try:
        if str(user_id) in USER_SESSIONS:
            del USER_SESSIONS[str(user_id)]
            
            # Create updated environment variable format
            session_pairs = []
            for uid, sess in USER_SESSIONS.items():
                session_pairs.append(f"{uid}:{sess}")
            
            env_format = ",".join(session_pairs) if session_pairs else ""
            
            # Notify owners about removal
            for owner_id in OWNER_IDS:
                try:
                    await BOT_INSTANCE.send_message(
                        chat_id=owner_id,
                        text=(
                            f"🗑️ **User Session Removed**\n\n"
                            f"👤 **User ID:** `{user_id}`\n\n"
                            f"**Updated USER_SESSIONS env var:**\n"
                            f"`USER_SESSIONS={env_format}`\n\n"
                            f"Total active sessions: {len(USER_SESSIONS)}"
                        ),
                        parse_mode="Markdown"
                    )
                except Exception as e:
                    logger.error(f"Failed to notify owner {owner_id} about session removal: {e}")
            
            logger.info(f"Removed session for user {user_id}. Total sessions: {len(USER_SESSIONS)}")
            
    except Exception as e:
        logger.exception(f"Error removing user session from env: {e}")


async def getstrings_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Owner-only: Get all user session strings in env var format"""
    user_id = update.effective_user.id
    
    if user_id not in OWNER_IDS:
        await update.message.reply_text(
            "❌ **Owner Only**\n\nThis command is only available to owners.",
            parse_mode="Markdown"
        )
        return
    
    if not USER_SESSIONS:
        await update.message.reply_text(
            "📭 **No User Sessions**\n\nNo active user sessions found.",
            parse_mode="Markdown"
        )
        return
    
    # Create detailed session info
    session_details = []
    for uid_str, session in USER_SESSIONS.items():
        try:
            uid = int(uid_str)
            # Try to get user info from database
            user_info = await db_call(db.get_user, uid)
            if user_info:
                phone = user_info.get("phone", "Unknown")
                name = user_info.get("name", "Unknown")
                status = "✅ Connected" if user_info.get("is_logged_in") else "❌ Disconnected"
                session_details.append(
                    f"👤 **User ID:** `{uid}`\n"
                    f"📱 **Phone:** `{phone}`\n"
                    f"👤 **Name:** `{name}`\n"
                    f"🔗 **Status:** {status}\n"
                    f"🔑 **Session:** `{session[:50]}...`\n"
                )
            else:
                session_details.append(
                    f"👤 **User ID:** `{uid}`\n"
                    f"🔑 **Session:** `{session[:50]}...`\n"
                )
        except Exception as e:
            logger.error(f"Error getting info for user {uid_str}: {e}")
            session_details.append(f"👤 **User ID:** `{uid_str}`\n🔑 **Session:** `{session[:50]}...`\n")
    
    # Create environment variable format
    session_pairs = []
    for uid, sess in USER_SESSIONS.items():
        session_pairs.append(f"{uid}:{sess}")
    
    env_format = ",".join(session_pairs)
    
    # Send in multiple messages if too long
    message_parts = []
    current_part = ""
    
    # Part 1: Session details
    details_text = f"🔑 **Active User Sessions ({len(USER_SESSIONS)})**\n\n"
    details_text += "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n".join(session_details)
    
    # Part 2: Environment variable format
    env_text = f"\n📋 **Environment Variable Format:**\n\n```\nUSER_SESSIONS={env_format}\n```\n\n"
    env_text += f"💡 **Copy and paste this into your .env file or Render environment variables**"
    
    # Send details
    await update.message.reply_text(
        details_text,
        parse_mode="Markdown"
    )
    
    # Send env format (might be long)
    if len(env_text) > 4000:
        # Split if too long
        chunks = [env_text[i:i+4000] for i in range(0, len(env_text), 4000)]
        for chunk in chunks:
            await update.message.reply_text(
                f"```\n{chunk}\n```",
                parse_mode="Markdown"
            )
    else:
        await update.message.reply_text(
            env_text,
            parse_mode="Markdown"
        )


# ---------- Duplicate Detection ----------
def create_message_hash(message_text: str, sender_id: Optional[int] = None) -> str:
    """Create a hash for message duplicate detection"""
    content = message_text.strip().lower()
    if sender_id:
        content = f"{sender_id}:{content}"
    return hashlib.sha256(content.encode()).hexdigest()[:16]


def is_duplicate_message(user_id: int, chat_id: int, message_hash: str) -> bool:
    """Check if message is a duplicate within the time window"""
    key = (user_id, chat_id)
    if key not in message_history:
        return False
    
    current_time = time.time()
    # Clean old messages outside the window
    message_history[key] = [
        (h, t, txt) for h, t, txt in message_history[key]
        if current_time - t <= DUPLICATE_CHECK_WINDOW
    ]
    
    # Check for duplicate
    for stored_hash, _, _ in message_history[key]:
        if stored_hash == message_hash:
            return True
    
    return False


def store_message_hash(user_id: int, chat_id: int, message_hash: str, message_text: str):
    """Store message hash for duplicate checking"""
    key = (user_id, chat_id)
    if key not in message_history:
        message_history[key] = []
    
    # Add new hash
    message_history[key].append((message_hash, time.time(), message_text[:100]))
    
    # Limit stored messages
    if len(message_history[key]) > MESSAGE_HASH_LIMIT:
        message_history[key] = message_history[key][-MESSAGE_HASH_LIMIT:]


# ---------- Authorization helpers ----------
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
            await update.message.reply_text(
                UNAUTHORIZED_MESSAGE,
                parse_mode="Markdown",
                disable_web_page_preview=True,
            )
        elif update.callback_query:
            await update.callback_query.answer()
            await update.callback_query.message.reply_text(
                UNAUTHORIZED_MESSAGE,
                parse_mode="Markdown",
                disable_web_page_preview=True,
            )
        return False

    return True


# ---------- Simple UI handlers ----------
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
╔══════════════════════════════╗
║   🔍 DUPLICATE MONITOR BOT   ║
║  Telegram Message Monitoring  ║
╚══════════════════════════════╝

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

👤 **User:** {user_name}
📱 **Phone:** `{user_phone}`
{status_emoji} **Status:** {status_text}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📋 **COMMANDS:**

🔐 **Account Management:**
  /login - Connect your Telegram account
  /logout - Disconnect your account

🔍 **Monitoring Tasks:**
  /monitoradd - Create a new monitoring task
  /monitortasks - List all your tasks

🆔 **Utilities:**
  /getallid - Get all your chat IDs

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

⚙️ **How it works:**
1. Connect your account with /login
2. Create a monitoring task for chats
3. Bot detects duplicate messages
4. Get notified and reply manually!

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

    keyboard = []
    if is_logged_in:
        keyboard.append([InlineKeyboardButton("📋 My Monitored Chats", callback_data="show_tasks")])
        keyboard.append([InlineKeyboardButton("🔴 Disconnect", callback_data="logout")])
    else:
        keyboard.append([InlineKeyboardButton("🟢 Connect Account", callback_data="login")])

    await update.message.reply_text(
        message_text,
        reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
        parse_mode="Markdown",
    )


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
    elif query.data.startswith("chatids_"):
        user_id = query.from_user.id
        if query.data == "chatids_back":
            await show_chat_categories(user_id, query.message.chat.id, query.message.message_id, context)
        else:
            parts = query.data.split("_")
            category = parts[1]
            page = int(parts[2])
            await show_categorized_chats(user_id, query.message.chat.id, query.message.message_id, category, page, context)
    elif query.data.startswith("task_"):
        await handle_task_menu(update, context)
    elif query.data.startswith("toggle_"):
        await handle_toggle_action(update, context)
    elif query.data.startswith("delete_"):
        await handle_delete_action(update, context)
    elif query.data.startswith("confirm_delete_"):
        await handle_confirm_delete(update, context)
    elif query.data.startswith("reply_"):
        await handle_reply_action(update, context)


# ---------- Task creation flow ----------
async def monitoradd_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start the interactive task creation process"""
    user_id = update.effective_user.id

    if not await check_authorization(update, context):
        return

    user = await db_call(db.get_user, user_id)
    if not user or not user["is_logged_in"]:
        await update.message.reply_text(
            "❌ **You need to connect your account first!**\n\nUse /login to connect your Telegram account.",
            parse_mode="Markdown"
        )
        return

    task_creation_states[user_id] = {
        "step": "waiting_name",
        "name": "",
        "chat_ids": []
    }

    await update.message.reply_text(
        "🎯 **Let's create a new monitoring task!**\n\n"
        "📝 **Step 1 of 2:** Please enter a name for your monitoring task.\n\n"
        "💡 *Example: Group Duplicate Checker*",
        parse_mode="Markdown"
    )


async def handle_task_creation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle interactive task creation steps"""
    user_id = update.effective_user.id
    text = update.message.text.strip()

    if user_id not in task_creation_states:
        return

    state = task_creation_states[user_id]

    try:
        if state["step"] == "waiting_name":
            if not text:
                await update.message.reply_text("❌ **Please enter a valid task name!**")
                return

            state["name"] = text
            state["step"] = "waiting_chats"

            await update.message.reply_text(
                f"✅ **Task name saved:** {text}\n\n"
                "📥 **Step 2 of 2:** Please enter the chat ID(s) to monitor.\n\n"
                "You can enter multiple IDs separated by spaces.\n"
                "💡 *Use /getallid to find your chat IDs*\n\n"
                "**Example:** `-1001234567890 -1009876543210`",
                parse_mode="Markdown"
            )

        elif state["step"] == "waiting_chats":
            if not text:
                await update.message.reply_text("❌ **Please enter at least one chat ID!**")
                return

            try:
                chat_ids = [int(id_str.strip()) for id_str in text.split() if id_str.strip().lstrip('-').isdigit()]
                if not chat_ids:
                    await update.message.reply_text("❌ **Please enter valid numeric IDs!**")
                    return

                state["chat_ids"] = chat_ids

                task_settings = {
                    "check_duplicate_and_notify": True,
                    "manual_reply_system": True,
                    "auto_reply_system": False,
                    "auto_reply_message": "",
                    "outgoing_message_monitoring": True
                }

                added = await db_call(db.add_monitoring_task, 
                                     user_id, 
                                     state["name"], 
                                     state["chat_ids"],
                                     task_settings)

                if added:
                    tasks_cache.setdefault(user_id, [])
                    tasks_cache[user_id].append({
                        "id": None,
                        "label": state["name"],
                        "chat_ids": state["chat_ids"],
                        "is_active": 1,
                        "settings": task_settings
                    })

                    await update.message.reply_text(
                        f"🎉 **Monitoring task created successfully!**\n\n"
                        f"📋 **Name:** {state['name']}\n"
                        f"📥 **Monitoring Chats:** {', '.join(map(str, state['chat_ids']))}\n\n"
                        "✅ Default settings applied:\n"
                        "• Check Duo & Notify: ✅ Active\n"
                        "• Manual reply system: ✅ Enabled\n"
                        "• Auto Reply system: ❌ Disabled\n"
                        "• Outgoing Message monitoring: ✅ Enabled\n\n"
                        "Use /monitortasks to manage your task!",
                        parse_mode="Markdown"
                    )

                    logger.info(f"Task created for user {user_id}: {state['name']} monitoring chats {state['chat_ids']}")

                    # Update event handlers for the new chats
                    if user_id in user_clients:
                        await update_monitoring_for_user(user_id)

                    del task_creation_states[user_id]

                else:
                    await update.message.reply_text(
                        f"❌ **Task '{state['name']}' already exists!**\n\n"
                        "Please choose a different name.",
                        parse_mode="Markdown"
                    )

            except ValueError:
                await update.message.reply_text("❌ **Please enter valid numeric IDs only!**")

    except Exception as e:
        logger.exception("Error in task creation for user %s: %s", user_id, e)
        await update.message.reply_text(
            f"❌ **Error creating task:** {str(e)}\n\n"
            "Please try again with /monitoradd",
            parse_mode="Markdown"
        )
        if user_id in task_creation_states:
            del task_creation_states[user_id]


# ---------- Task Menu System ----------
async def monitortasks_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List all tasks with inline buttons"""
    user_id = update.effective_user.id if update.effective_user else update.callback_query.from_user.id

    if not await check_authorization(update, context):
        return

    message = update.message if update.message else update.callback_query.message
    tasks = tasks_cache.get(user_id) or []

    if not tasks:
        await message.reply_text(
            "📋 **No Active Monitoring Tasks**\n\n"
            "You don't have any monitoring tasks yet.\n\n"
            "Create one with:\n"
            "/monitoradd",
            parse_mode="Markdown"
        )
        return

    task_list = "📋 **Your Monitoring Tasks**\n\n"
    task_list += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    
    keyboard = []
    
    for i, task in enumerate(tasks, 1):
        task_list += f"{i}. **{task['label']}**\n"
        task_list += f"   📥 Monitoring: {', '.join(map(str, task['chat_ids']))}\n\n"
        
        keyboard.append([InlineKeyboardButton(f"{i}. {task['label']}", callback_data=f"task_{task['label']}")])

    task_list += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    task_list += f"Total: **{len(tasks)} task(s)**\n\n"
    task_list += "💡 **Tap any task below to manage it!**"

    await message.reply_text(
        task_list,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )


async def handle_task_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show task management menu"""
    query = update.callback_query
    user_id = query.from_user.id
    task_label = query.data.replace("task_", "")
    
    user_tasks = tasks_cache.get(user_id, [])
    task = None
    for t in user_tasks:
        if t["label"] == task_label:
            task = t
            break
    
    if not task:
        await query.answer("Task not found!", show_alert=True)
        return
    
    settings = task.get("settings", {})
    
    check_duo_emoji = "✅" if settings.get("check_duplicate_and_notify", True) else "❌"
    manual_reply_emoji = "✅" if settings.get("manual_reply_system", True) else "❌"
    auto_reply_emoji = "✅" if settings.get("auto_reply_system", False) else "❌"
    outgoing_emoji = "✅" if settings.get("outgoing_message_monitoring", True) else "❌"
    
    # Get auto reply message for display
    auto_reply_message = settings.get("auto_reply_message", "")
    if auto_reply_message:
        auto_reply_display = f"Auto Reply = '{auto_reply_message[:30]}{'...' if len(auto_reply_message) > 30 else ''}'"
    else:
        auto_reply_display = "Auto Reply = Off"
    
    message_text = f"🔧 **Task Management: {task_label}**\n\n"
    message_text += f"📥 **Monitoring Chats:** {', '.join(map(str, task['chat_ids']))}\n\n"
    message_text += "⚙️ **Settings:**\n"
    message_text += f"{check_duo_emoji} Check Duo & Notify - Detects duplicates and sends alerts\n"
    message_text += f"{manual_reply_emoji} Manual reply system - Allows manual replies to duplicates\n"
    message_text += f"{auto_reply_emoji} {auto_reply_display}\n"
    message_text += f"{outgoing_emoji} Outgoing Message monitoring - Monitors your outgoing messages\n\n"
    message_text += "💡 **Tap any option below to change it!**"
    
    keyboard = [
        [
            InlineKeyboardButton(f"{check_duo_emoji} Check Duo & Notify", callback_data=f"toggle_{task_label}_check_duplicate_and_notify"),
            InlineKeyboardButton(f"{manual_reply_emoji} Manual Reply", callback_data=f"toggle_{task_label}_manual_reply_system")
        ],
        [
            InlineKeyboardButton(f"{auto_reply_emoji} Auto Reply", callback_data=f"toggle_{task_label}_auto_reply_system"),
            InlineKeyboardButton(f"{outgoing_emoji} Outgoing", callback_data=f"toggle_{task_label}_outgoing_message_monitoring")
        ],
        [InlineKeyboardButton("🗑️ Delete", callback_data=f"delete_{task_label}")],
        [InlineKeyboardButton("🔙 Back to Tasks", callback_data="show_tasks")]
    ]
    
    await query.edit_message_text(
        message_text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )


async def handle_toggle_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle toggle actions for settings with instant button updates"""
    query = update.callback_query
    user_id = query.from_user.id
    data_parts = query.data.replace("toggle_", "").split("_")
    
    if len(data_parts) < 2:
        await query.answer("Invalid action!", show_alert=True)
        return
    
    task_label = data_parts[0]
    toggle_type = "_".join(data_parts[1:])
    
    user_tasks = tasks_cache.get(user_id, [])
    task_index = -1
    for i, t in enumerate(user_tasks):
        if t["label"] == task_label:
            task_index = i
            break
    
    if task_index == -1:
        await query.answer("Task not found!", show_alert=True)
        return
    
    task = user_tasks[task_index]
    settings = task.get("settings", {})
    new_state = None
    status_text = ""
    
    # Determine which setting is being toggled
    if toggle_type == "check_duplicate_and_notify":
        new_state = not settings.get("check_duplicate_and_notify", True)
        settings["check_duplicate_and_notify"] = new_state
        status_text = "Check Duo & Notify"
        
    elif toggle_type == "manual_reply_system":
        new_state = not settings.get("manual_reply_system", True)
        settings["manual_reply_system"] = new_state
        status_text = "Manual reply system"
        
    elif toggle_type == "auto_reply_system":
        current_state = settings.get("auto_reply_system", False)
        
        if not current_state:
            # If turning ON, ask for auto reply message
            context.user_data[f"waiting_auto_reply_{task_label}"] = True
            await query.edit_message_text(
                f"🤖 **Auto Reply Setup for: {task_label}**\n\n"
                "Please enter the message you want to use for auto reply.\n\n"
                "⚠️ **Important:** This message will be sent automatically whenever a duplicate is detected.\n"
                "It will appear as coming from your account.\n\n"
                "💡 **Example messages:**\n"
                "• 'Please avoid sending duplicate messages.'\n"
                "• 'This message was already sent.'\n"
                "• 'Duplicate detected.'\n\n"
                "**Type your auto reply message now:**",
                parse_mode="Markdown"
            )
            return
        else:
            # If turning OFF, just disable it
            new_state = False
            settings["auto_reply_system"] = new_state
            settings["auto_reply_message"] = ""
            status_text = "Auto Reply system"
        
    elif toggle_type == "outgoing_message_monitoring":
        new_state = not settings.get("outgoing_message_monitoring", True)
        settings["outgoing_message_monitoring"] = new_state
        status_text = "Outgoing message monitoring"
        
    else:
        await query.answer(f"Unknown toggle type: {toggle_type}")
        return
    
    # Update cache with new state
    if new_state is not None:
        task["settings"] = settings
        tasks_cache[user_id][task_index] = task
    
    # Update the button inline if not auto_reply_system (which was handled above)
    if toggle_type != "auto_reply_system":
        keyboard = query.message.reply_markup.inline_keyboard
        button_found = False
        new_emoji = "✅" if new_state else "❌"
        
        # Create a new keyboard with updated button
        new_keyboard = []
        for row in keyboard:
            new_row = []
            for button in row:
                if button.callback_data == query.data:
                    # Update this button
                    current_text = button.text
                    # Extract the text after the emoji
                    if "✅ " in current_text:
                        text_without_emoji = current_text.split("✅ ", 1)[1]
                        new_text = f"{new_emoji} {text_without_emoji}"
                    elif "❌ " in current_text:
                        text_without_emoji = current_text.split("❌ ", 1)[1]
                        new_text = f"{new_emoji} {text_without_emoji}"
                    elif current_text.startswith("✅"):
                        text_without_emoji = current_text[1:]
                        new_text = f"{new_emoji}{text_without_emoji}"
                    elif current_text.startswith("❌"):
                        text_without_emoji = current_text[1:]
                        new_text = f"{new_emoji}{text_without_emoji}"
                    else:
                        # Fallback - preserve the button text but change emoji
                        new_text = f"{new_emoji} {current_text}"
                    
                    new_row.append(InlineKeyboardButton(new_text, callback_data=query.data))
                    button_found = True
                else:
                    new_row.append(button)
            new_keyboard.append(new_row)
        
        # Update the message inline if button was found
        if button_found:
            try:
                await query.edit_message_reply_markup(
                    reply_markup=InlineKeyboardMarkup(new_keyboard)
                )
                status_display = "✅ Active" if new_state else "❌ Inactive"
                await query.answer(f"{status_text}: {status_display}")
            except Exception as e:
                logger.exception("Error updating inline keyboard: %s", e)
                status_display = "✅ Active" if new_state else "❌ Inactive"
                await query.answer(f"{status_text}: {status_display}")
                await handle_task_menu(update, context)
        else:
            status_display = "✅ Active" if new_state else "❌ Inactive"
            await query.answer(f"{status_text}: {status_display}")
            await handle_task_menu(update, context)
    
    # Update database in background
    try:
        if new_state is not None or toggle_type == "auto_reply_system":
            asyncio.create_task(
                db_call(db.update_task_settings, user_id, task_label, settings)
            )
            logger.info(f"Updated task {task_label} setting {toggle_type} to {new_state} for user {user_id}")
    except Exception as e:
        logger.exception("Error updating task settings in DB: %s", e)


async def handle_auto_reply_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle auto reply message input"""
    user_id = update.effective_user.id
    text = update.message.text.strip()
    
    # Check if we're waiting for auto reply message
    waiting_for_auto_reply = False
    task_label = None
    
    for key in list(context.user_data.keys()):
        if key.startswith("waiting_auto_reply_"):
            waiting_for_auto_reply = True
            task_label = key.replace("waiting_auto_reply_", "")
            del context.user_data[key]
            break
    
    if not waiting_for_auto_reply or not task_label:
        return
    
    # Find the task
    user_tasks = tasks_cache.get(user_id, [])
    task_index = -1
    for i, t in enumerate(user_tasks):
        if t["label"] == task_label:
            task_index = i
            break
    
    if task_index == -1:
        await update.message.reply_text("❌ Task not found!")
        return
    
    task = user_tasks[task_index]
    settings = task.get("settings", {})
    
    # Update settings with auto reply message
    settings["auto_reply_system"] = True
    settings["auto_reply_message"] = text
    
    # Update cache
    task["settings"] = settings
    tasks_cache[user_id][task_index] = task
    
    # Update database
    try:
        await db_call(db.update_task_settings, user_id, task_label, settings)
    except Exception as e:
        logger.exception("Error updating task settings in DB: %s", e)
        await update.message.reply_text("❌ Error saving auto reply message!")
        return
    
    # Send confirmation
    await update.message.reply_text(
        f"✅ **Auto Reply Message Added Successfully!**\n\n"
        f"Task: **{task_label}**\n"
        f"Auto Reply Message: '{text}'\n\n"
        "This message will be sent automatically whenever a duplicate is detected.\n"
        "⚠️ **Remember:** It will appear as coming from your account.",
        parse_mode="Markdown"
    )
    
    logger.info(f"Auto reply message set for task {task_label} by user {user_id}")


async def handle_notification_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle reply to notification message (using swipe-left reply)"""
    user_id = update.effective_user.id
    text = update.message.text.strip()
    
    # Check if this is a reply to a message
    if not update.message.reply_to_message:
        return
    
    replied_message_id = update.message.reply_to_message.message_id
    
    # Check if this is a notification message
    if replied_message_id not in notification_messages:
        return
    
    notification_data = notification_messages[replied_message_id]
    
    # Check if this reply is from the same user
    if notification_data["user_id"] != user_id:
        return
    
    # Get task details
    task_label = notification_data["task_label"]
    chat_id = notification_data["chat_id"]
    original_message_id = notification_data["original_message_id"]
    message_preview = notification_data.get("message_preview", "Unknown message")
    
    # Find the task
    user_tasks = tasks_cache.get(user_id, [])
    task = None
    for t in user_tasks:
        if t["label"] == task_label:
            task = t
            break
    
    if not task:
        await update.message.reply_text("❌ Task not found!")
        return
    
    # Check if user is logged in
    if user_id not in user_clients:
        await update.message.reply_text("❌ You need to be logged in to send replies!")
        return
    
    client = user_clients[user_id]
    
    try:
        # Get the chat entity
        chat_entity = await client.get_input_entity(chat_id)
        
        # Send the reply
        await client.send_message(
            chat_entity,
            text,
            reply_to=original_message_id
        )
        
        # Escape text for Markdown
        escaped_text = escape_markdown(text, version=2)
        escaped_preview = escape_markdown(message_preview, version=2)
        
        # Confirm to user
        await update.message.reply_text(
            f"✅ **Reply sent successfully!**\n\n"
            f"📝 **Your reply:** {escaped_text}\n"
            f"🔗 **Replying to:** `{escaped_preview}`\n\n"
            "The duplicate sender has been notified with your reply.",
            parse_mode="Markdown"
        )
        
        # Log the action
        logger.info(f"User {user_id} sent manual reply to duplicate in chat {chat_id}")
        
        # Remove notification from tracking
        notification_messages.pop(replied_message_id, None)
        
    except Exception as e:
        logger.exception(f"Error sending manual reply for user {user_id}: {e}")
        await update.message.reply_text(
            f"❌ **Failed to send reply:** {str(e)}\n\n"
            "Please try again or check your connection.",
            parse_mode="Markdown"
        )


async def handle_delete_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle task deletion"""
    query = update.callback_query
    user_id = query.from_user.id
    task_label = query.data.replace("delete_", "")
    
    message_text = f"🗑️ **Delete Monitoring Task: {task_label}**\n\n"
    message_text += "⚠️ **Are you sure you want to delete this task?**\n\n"
    message_text += "This action cannot be undone!\n"
    message_text += "All monitoring will stop immediately."
    
    keyboard = [
        [
            InlineKeyboardButton("✅ Yes, Delete", callback_data=f"confirm_delete_{task_label}"),
            InlineKeyboardButton("❌ Cancel", callback_data=f"task_{task_label}")
        ]
    ]
    
    await query.edit_message_text(
        message_text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )


async def handle_confirm_delete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Confirm and execute task deletion"""
    query = update.callback_query
    user_id = query.from_user.id
    task_label = query.data.replace("confirm_delete_", "")
    
    deleted = await db_call(db.remove_monitoring_task, user_id, task_label)
    
    if deleted:
        if user_id in tasks_cache:
            tasks_cache[user_id] = [t for t in tasks_cache[user_id] if t.get("label") != task_label]
        
        # Update event handlers after deletion
        if user_id in user_clients:
            await update_monitoring_for_user(user_id)
        
        await query.edit_message_text(
            f"✅ **Task '{task_label}' deleted successfully!**\n\n"
            "All monitoring for this task has been stopped.",
            parse_mode="Markdown"
        )
    else:
        await query.edit_message_text(
            f"❌ **Task '{task_label}' not found!**",
            parse_mode="Markdown"
        )


# ---------- Login/logout commands ----------
async def login_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id if update.effective_user else update.callback_query.from_user.id

    if not await check_authorization(update, context):
        return

    message = update.message if update.message else update.callback_query.message

    if len(user_clients) >= MAX_CONCURRENT_USERS:
        await message.reply_text(
            "❌ **Server at capacity!**\n\n"
            "Too many users are currently connected. Please try again later.",
            parse_mode="Markdown",
        )
        return

    user = await db_call(db.get_user, user_id)
    if user and user.get("is_logged_in"):
        await message.reply_text(
            "✅ **You are already logged in!**\n\n"
            f"📱 Phone: `{user['phone']}`\n"
            f"👤 Name: `{user['name']}`\n\n"
            "Use /logout if you want to disconnect.",
            parse_mode="Markdown",
        )
        return

    # Check if we have a saved session for this user
    if str(user_id) in USER_SESSIONS:
        session_string = USER_SESSIONS[str(user_id)]
        try:
            client = TelegramClient(
                StringSession(session_string),
                API_ID,
                API_HASH,
                device_model="Duplicate Monitor Bot",
                system_version="1.0",
                app_version="1.0",
                lang_code="en"
            )
            await client.connect()
            
            if await client.is_user_authorized():
                me = await client.get_me()
                user_clients[user_id] = client
                tasks_cache.setdefault(user_id, [])
                chat_entity_cache.setdefault(user_id, {})
                await start_monitoring_for_user(user_id)
                
                await db_call(db.save_user, user_id, me.phone, me.first_name, session_string, True)
                
                await message.reply_text(
                    f"✅ **Session Restored Successfully!** 🎉\n\n"
                    f"👤 **Name:** {me.first_name or 'User'}\n"
                    f"📱 **Phone:** `{me.phone}`\n"
                    f"🆔 **User ID:** `{me.id}`\n\n"
                    "Your previous session has been restored from saved string session.",
                    parse_mode="Markdown",
                )
                logger.info(f"User {user_id} restored from saved session")
                return
            else:
                logger.warning(f"Saved session for user {user_id} is no longer valid")
        except Exception as e:
            logger.exception(f"Error restoring session for user {user_id}: {e}")
    
    # If no saved session or invalid, start new login
    client = TelegramClient(
        StringSession(),
        API_ID,
        API_HASH,
        device_model="Duplicate Monitor Bot",
        system_version="1.0",
        app_version="1.0",
        lang_code="en"
    )
    
    try:
        await client.connect()
        logger.info(f"Telethon client connected for user {user_id}")
    except Exception as e:
        logger.error(f"Telethon connection failed for user {user_id}: {e}")
        await message.reply_text(
            f"❌ **Connection failed:** {str(e)}\n\n"
            "Please try again in a few minutes.",
            parse_mode="Markdown",
        )
        return

    login_states[user_id] = {"client": client, "step": "waiting_phone"}

    await message.reply_text(
        "📱 **Login Process**\n\n"
        "1️⃣ **Enter your phone number** (with country code):\n\n"
        "**Examples:**\n"
        "• `+1234567890`\n"
        "• `+447911123456`\n"
        "• `+4915112345678`\n\n"
        "⚠️ **Important:**\n"
        "• Include the `+` sign\n"
        "• Use international format\n"
        "• No spaces or dashes\n\n"
        "If you don't receive a code, try:\n"
        "1. Check phone number format\n"
        "2. Wait 2 minutes between attempts\n"
        "3. Use the Telegram app to verify\n\n"
        "**Type your phone number now:**",
        parse_mode="Markdown",
    )


async def handle_login_process(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    # Check if we're in task creation
    if user_id in task_creation_states:
        await handle_task_creation(update, context)
        return
    
    # Check if we're waiting for auto reply message
    waiting_for_auto_reply = any(key.startswith("waiting_auto_reply_") for key in context.user_data.keys())
    if waiting_for_auto_reply:
        await handle_auto_reply_message(update, context)
        return
    
    # Check if we're replying to a notification
    if update.message.reply_to_message:
        await handle_notification_reply(update, context)
        return
    
    if user_id in logout_states:
        handled = await handle_logout_confirmation(update, context)
        if handled:
            return

    if user_id not in login_states:
        return

    state = login_states[user_id]
    text = update.message.text.strip()
    client = state["client"]

    try:
        if state["step"] == "waiting_phone":
            if not text.startswith('+'):
                await update.message.reply_text(
                    "❌ **Invalid format!**\n\n"
                    "Phone number must start with `+`\n"
                    "Example: `+1234567890`\n\n"
                    "Please enter your phone number again:",
                    parse_mode="Markdown",
                )
                return
            
            clean_phone = ''.join(c for c in text if c.isdigit() or c == '+')
            
            if len(clean_phone) < 8:
                await update.message.reply_text(
                    "❌ **Invalid phone number!**\n\n"
                    "Phone number seems too short. Please check and try again.\n"
                    "Example: `+1234567890`",
                    parse_mode="Markdown",
                )
                return

            processing_msg = await update.message.reply_text(
                "⏳ **Sending verification code...**\n\n"
                "This may take a few seconds. Please wait...",
                parse_mode="Markdown",
            )

            try:
                logger.info(f"Sending code request to {clean_phone} for user {user_id}")
                result = await client.send_code_request(clean_phone)
                logger.info(f"Code request result received for {clean_phone}")
                
                state["phone"] = clean_phone
                state["phone_code_hash"] = result.phone_code_hash
                state["step"] = "waiting_code"

                await processing_msg.edit_text(
                    f"✅ **Verification code sent!**\n\n"
                    f"📱 **Code sent to:** `{clean_phone}`\n\n"
                    "2️⃣ **Enter the verification code:**\n\n"
                    "**Format:** `verify12345`\n"
                    "• Type `verify` followed by your 5-digit code\n"
                    "• No spaces, no brackets\n\n"
                    "**Example:** If your code is `54321`, type:\n"
                    "`verify54321`\n\n"
                    "⚠️ **If you don't receive the code:**\n"
                    "1. Check your Telegram app notifications\n"
                    "2. Wait 2-3 minutes\n"
                    "3. Check spam messages\n"
                    "4. Try login via Telegram app first",
                    parse_mode="Markdown",
                )

            except Exception as e:
                error_msg = str(e)
                logger.error(f"Error sending code for user {user_id}: {error_msg}")
                
                if "PHONE_NUMBER_INVALID" in error_msg:
                    error_text = "❌ **Invalid phone number!**\n\nPlease check the format and try again."
                elif "PHONE_NUMBER_BANNED" in error_msg:
                    error_text = "❌ **Phone number banned!**\n\nThis phone number cannot be used."
                elif "FLOOD" in error_msg or "Too many" in error_msg:
                    error_text = "❌ **Too many attempts!**\n\nPlease wait 2-3 minutes before trying again."
                elif "PHONE_CODE_EXPIRED" in error_msg:
                    error_text = "❌ **Code expired!**\n\nPlease start over with /login."
                else:
                    error_text = f"❌ **Error:** {error_msg}\n\nPlease try again in a few minutes."
                
                await processing_msg.edit_text(
                    error_text + "\n\nUse /login to try again.",
                    parse_mode="Markdown",
                )
                
                try:
                    await client.disconnect()
                except:
                    pass
                
                if user_id in login_states:
                    del login_states[user_id]
                return

        elif state["step"] == "waiting_code":
            if not text.startswith("verify"):
                await update.message.reply_text(
                    "❌ **Invalid format!**\n\n"
                    "Please use the format: `verify12345`\n\n"
                    "Type `verify` followed immediately by your 5-digit code.\n"
                    "**Example:** `verify54321`",
                    parse_mode="Markdown",
                )
                return

            code = text[6:]
            
            if not code or not code.isdigit():
                await update.message.reply_text(
                    "❌ **Invalid code!**\n\n"
                    "Code must contain only digits.\n"
                    "**Example:** `verify12345`",
                    parse_mode="Markdown",
                )
                return
            
            if len(code) != 5:
                await update.message.reply_text(
                    "❌ **Code must be 5 digits!**\n\n"
                    f"Your code has {len(code)} digits. Please check and try again.\n"
                    "**Example:** `verify12345`",
                    parse_mode="Markdown",
                )
                return

            verifying_msg = await update.message.reply_text(
                "🔄 **Verifying code...**\n\nPlease wait...",
                parse_mode="Markdown",
            )

            try:
                await client.sign_in(state["phone"], code, phone_code_hash=state["phone_code_hash"])

                me = await client.get_me()
                session_string = client.session.save()

                # Save to database
                await db_call(db.save_user, user_id, state["phone"], me.first_name, session_string, True)

                # Save to environment variable system
                await save_user_session_to_env(user_id, session_string, state["phone"], me.first_name)

                user_clients[user_id] = client
                tasks_cache.setdefault(user_id, [])
                chat_entity_cache.setdefault(user_id, {})
                await start_monitoring_for_user(user_id)

                del login_states[user_id]

                await verifying_msg.edit_text(
                    "✅ **Successfully connected!** 🎉\n\n"
                    f"👤 **Name:** {me.first_name or 'User'}\n"
                    f"📱 **Phone:** `{state['phone']}`\n"
                    f"🆔 **User ID:** `{me.id}`\n\n"
                    "**Session has been saved!** 🔒\n"
                    "Your login will survive bot restarts.\n\n"
                    "**Now you can:**\n"
                    "• Create monitoring tasks with /monitoradd\n"
                    "• View your tasks with /monitortasks\n"
                    "• Get chat IDs with /getallid\n\n"
                    "Welcome aboard! 🚀",
                    parse_mode="Markdown",
                )
                
                logger.info(f"User {user_id} successfully logged in as {me.first_name}")

            except SessionPasswordNeededError:
                state["step"] = "waiting_2fa"
                await verifying_msg.edit_text(
                    "🔐 **2-Step Verification Required**\n\n"
                    "This account has 2FA enabled for extra security.\n\n"
                    "3️⃣ **Enter your 2FA password:**\n\n"
                    "**Format:** `passwordYourPassword123`\n"
                    "• Type `password` followed by your 2FA password\n"
                    "• No spaces, no brackets\n\n"
                    "**Example:** If your password is `mypass123`, type:\n"
                    "`passwordmypass123`",
                    parse_mode="Markdown",
                )
            except Exception as e:
                error_msg = str(e)
                logger.error(f"Error verifying code for user {user_id}: {error_msg}")
                
                if "PHONE_CODE_INVALID" in error_msg:
                    error_text = "❌ **Invalid code!**\n\nPlease check the code and try again."
                elif "PHONE_CODE_EXPIRED" in error_msg:
                    error_text = "❌ **Code expired!**\n\nPlease request a new code with /login."
                else:
                    error_text = f"❌ **Verification failed:** {error_msg}"
                
                await verifying_msg.edit_text(
                    error_text + "\n\nUse /login to try again.",
                    parse_mode="Markdown",
                )

        elif state["step"] == "waiting_2fa":
            if not text.startswith("password"):
                await update.message.reply_text(
                    "❌ **Invalid format!**\n\n"
                    "Please use the format: `passwordYourPassword123`\n\n"
                    "Type `password` followed immediately by your 2FA password.\n"
                    "**Example:** `passwordmypass123`",
                    parse_mode="Markdown",
                )
                return

            password = text[8:]

            if not password:
                await update.message.reply_text(
                    "❌ **No password provided!**\n\n"
                    "Please type `password` followed by your 2FA password.\n"
                    "**Example:** `passwordmypass123`",
                    parse_mode="Markdown",
                )
                return

            verifying_msg = await update.message.reply_text(
                "🔄 **Verifying 2FA password...**\n\nPlease wait...",
                parse_mode="Markdown",
            )

            try:
                await client.sign_in(password=password)

                me = await client.get_me()
                session_string = client.session.save()

                # Save to database
                await db_call(db.save_user, user_id, state["phone"], me.first_name, session_string, True)

                # Save to environment variable system
                await save_user_session_to_env(user_id, session_string, state["phone"], me.first_name)

                user_clients[user_id] = client
                tasks_cache.setdefault(user_id, [])
                chat_entity_cache.setdefault(user_id, {})
                await start_monitoring_for_user(user_id)

                del login_states[user_id]

                await verifying_msg.edit_text(
                    "✅ **Successfully connected with 2FA!** 🎉\n\n"
                    f"👤 **Name:** {me.first_name or 'User'}\n"
                    f"📱 **Phone:** `{state['phone']}`\n"
                    f"🆔 **User ID:** `{me.id}`\n\n"
                    "**Session has been saved!** 🔒\n"
                    "Your login will survive bot restarts.\n\n"
                    "Your account is now securely connected! 🔐",
                    parse_mode="Markdown",
                )

            except Exception as e:
                error_msg = str(e)
                logger.error(f"Error verifying 2FA for user {user_id}: {error_msg}")
                
                if "PASSWORD_HASH_INVALID" in error_msg or "PASSWORD_INVALID" in error_msg:
                    error_text = "❌ **Invalid 2FA password!**\n\nPlease check your password and try again."
                else:
                    error_text = f"❌ **2FA verification failed:** {error_msg}"
                
                await verifying_msg.edit_text(
                    error_text + "\n\nUse /login to try again.",
                    parse_mode="Markdown",
                )

    except Exception as e:
        logger.exception("Unexpected error during login process for %s", user_id)
        await update.message.reply_text(
            f"❌ **Unexpected error:** {str(e)}\n\n"
            "Please try /login again.\n\n"
            "If the problem persists, contact support.",
            parse_mode="Markdown",
        )
        if user_id in login_states:
            try:
                c = login_states[user_id].get("client")
                if c:
                    await c.disconnect()
            except Exception:
                logger.exception("Error disconnecting client after failed login for %s", user_id)
            del login_states[user_id]


async def logout_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id if update.effective_user else update.callback_query.from_user.id

    if not await check_authorization(update, context):
        return

    message = update.message if update.message else update.callback_query.message

    user = await db_call(db.get_user, user_id)
    if not user or not user["is_logged_in"]:
        await message.reply_text(
            "❌ **You're not connected!**\n\n" "Use /login to connect your account.", parse_mode="Markdown"
        )
        return

    logout_states[user_id] = {"phone": user["phone"]}

    await message.reply_text(
        "⚠️ **Confirm Logout**\n\n"
        f"📱 **Enter your phone number to confirm disconnection:**\n\n"
        f"Your connected phone: `{user['phone']}`\n\n"
        "Type your phone number exactly to confirm logout.",
        parse_mode="Markdown",
    )


async def handle_logout_confirmation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    user_id = update.effective_user.id

    if user_id not in logout_states:
        return False

    text = update.message.text.strip()
    stored_phone = logout_states[user_id]["phone"]

    if text != stored_phone:
        await update.message.reply_text(
            "❌ **Phone number doesn't match!**\n\n"
            f"Expected: `{stored_phone}`\n"
            f"You entered: `{text}`\n\n"
            "Please try again or use /start to cancel.",
            parse_mode="Markdown",
        )
        return True

    if user_id in user_clients:
        client = user_clients[user_id]
        try:
            # Remove all handlers for this user
            if user_id in handler_registered:
                for handler in handler_registered[user_id]:
                    try:
                        client.remove_event_handler(handler)
                    except Exception:
                        logger.exception("Error removing event handler during logout for user %s", user_id)
                handler_registered.pop(user_id, None)

            await client.disconnect()
        except Exception:
            logger.exception("Error disconnecting client for user %s", user_id)
        finally:
            user_clients.pop(user_id, None)

    try:
        await db_call(db.save_user, user_id, None, None, None, False)
    except Exception:
        logger.exception("Error saving user logout state for %s", user_id)
    
    # Remove from environment variable system
    await remove_user_session_from_env(user_id)
    
    tasks_cache.pop(user_id, None)
    chat_entity_cache.pop(user_id, None)
    logout_states.pop(user_id, None)
    reply_states.pop(user_id, None)
    auto_reply_states.pop(user_id, None)

    await update.message.reply_text(
        "👋 **Account disconnected successfully!**\n\n"
        "✅ All your monitoring tasks have been stopped.\n"
        "🗑️ Your session string has been removed.\n"
        "🔄 Use /login to connect again.",
        parse_mode="Markdown",
    )
    return True


async def getallid_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if not await check_authorization(update, context):
        return

    user = await db_call(db.get_user, user_id)
    if not user or not user["is_logged_in"]:
        await update.message.reply_text("❌ **You need to connect your account first!**\n\n" "Use /login to connect.", parse_mode="Markdown")
        return

    await update.message.reply_text("🔄 **Fetching your chats...**")

    await show_chat_categories(user_id, update.message.chat.id, None, context)


# ---------- Chat listing functions ----------
async def show_chat_categories(user_id: int, chat_id: int, message_id: int, context: ContextTypes.DEFAULT_TYPE):
    if user_id not in user_clients:
        return

    message_text = (
        "🗂️ **Chat ID Categories**\n\n"
        "📋 Choose which type of chat IDs you want to see:\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "🤖 **Bots** - Bot accounts\n"
        "📢 **Channels** - Broadcast channels\n"
        "👥 **Groups** - Group chats\n"
        "👤 **Private** - Private conversations\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "💡 Select a category below:"
    )

    keyboard = [
        [InlineKeyboardButton("🤖 Bots", callback_data="chatids_bots_0"), InlineKeyboardButton("📢 Channels", callback_data="chatids_channels_0")],
        [InlineKeyboardButton("👥 Groups", callback_data="chatids_groups_0"), InlineKeyboardButton("👤 Private", callback_data="chatids_private_0")],
    ]

    if message_id:
        await context.bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=message_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
    else:
        await context.bot.send_message(chat_id=chat_id, text=message_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")


async def show_categorized_chats(user_id: int, chat_id: int, message_id: int, category: str, page: int, context: ContextTypes.DEFAULT_TYPE):
    from telethon.tl.types import User, Channel, Chat

    if user_id not in user_clients:
        return

    client = user_clients[user_id]

    categorized_dialogs = []
    async for dialog in client.iter_dialogs():
        entity = dialog.entity

        if category == "bots":
            if isinstance(entity, User) and entity.bot:
                categorized_dialogs.append(dialog)
        elif category == "channels":
            if isinstance(entity, Channel) and getattr(entity, "broadcast", False):
                categorized_dialogs.append(dialog)
        elif category == "groups":
            if isinstance(entity, (Channel, Chat)) and not (isinstance(entity, Channel) and getattr(entity, "broadcast", False)):
                categorized_dialogs.append(dialog)
        elif category == "private":
            if isinstance(entity, User) and not entity.bot:
                categorized_dialogs.append(dialog)

    PAGE_SIZE = 10
    total_pages = max(1, (len(categorized_dialogs) + PAGE_SIZE - 1) // PAGE_SIZE)
    start = page * PAGE_SIZE
    end = start + PAGE_SIZE
    page_dialogs = categorized_dialogs[start:end]

    category_emoji = {"bots": "🤖", "channels": "📢", "groups": "👥", "private": "👤"}
    category_name = {"bots": "Bots", "channels": "Channels", "groups": "Groups", "private": "Private Chats"}

    emoji = category_emoji.get(category, "💬")
    name = category_name.get(category, "Chats")

    if not categorized_dialogs:
        chat_list = f"{emoji} **{name}**\n\n"
        chat_list += f"📭 **No {name.lower()} found!**\n\n"
        chat_list += "Try another category."
    else:
        chat_list = f"{emoji} **{name}** (Page {page + 1}/{total_pages})\n\n"
        chat_list += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"

        for i, dialog in enumerate(page_dialogs, start + 1):
            chat_name = dialog.name[:30] if dialog.name else "Unknown"
            chat_list += f"{i}. **{chat_name}**\n"
            chat_list += f"   🆔 `{dialog.id}`\n\n"

        chat_list += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        chat_list += f"📊 Total: {len(categorized_dialogs)} {name.lower()}\n"
        chat_list += "💡 Tap to copy the ID!"

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


# ---------- Monitoring core ----------
async def update_monitoring_for_user(user_id: int):
    """Update event handlers when tasks change"""
    if user_id not in user_clients:
        return
    
    client = user_clients[user_id]
    
    # Remove existing handlers
    if user_id in handler_registered:
        for handler in handler_registered[user_id]:
            try:
                client.remove_event_handler(handler)
            except Exception:
                logger.exception("Error removing old handler for user %s", user_id)
        handler_registered[user_id] = []
    
    # Get all monitored chat IDs for this user
    monitored_chat_ids = set()
    user_tasks = tasks_cache.get(user_id, [])
    for task in user_tasks:
        monitored_chat_ids.update(task.get("chat_ids", []))
    
    if not monitored_chat_ids:
        logger.info(f"No monitored chats for user {user_id}")
        return
    
    # Create handler for each monitored chat
    for chat_id in monitored_chat_ids:
        await register_handler_for_chat(user_id, chat_id, client)
    
    logger.info(f"Updated monitoring for user {user_id}: {len(monitored_chat_ids)} chat(s)")


async def register_handler_for_chat(user_id: int, chat_id: int, client: TelegramClient):
    """Register handler for specific chat"""
    
    async def _monitor_chat_handler(event):
        try:
            await optimized_gc()
            
            message = event.message
            if not message:
                return
            
            # Skip reaction messages
            if hasattr(message, 'reactions') and message.reactions:
                return
                
            message_text = event.raw_text or message.message
            if not message_text:
                return

            sender_id = message.sender_id
            message_id = message.id
            message_outgoing = message.out

            logger.debug(f"Processing monitored chat {chat_id} for user {user_id}")
            
            # Find tasks that monitor this specific chat
            user_tasks = tasks_cache.get(user_id, [])
            for task in user_tasks:
                if chat_id not in task.get("chat_ids", []):
                    continue
                    
                settings = task.get("settings", {})
                task_label = task.get("label", "Unknown")
                
                # Check outgoing message monitoring
                if message_outgoing and not settings.get("outgoing_message_monitoring", True):
                    continue
                    
                # Check duplicate detection
                if settings.get("check_duplicate_and_notify", True):
                    message_hash = create_message_hash(message_text, sender_id)
                    
                    if is_duplicate_message(user_id, chat_id, message_hash):
                        # Duplicate found!
                        logger.info(f"DUPLICATE DETECTED: User {user_id}, Task {task_label}, Chat {chat_id}, Hash {message_hash}")
                        
                        # Check if auto reply is enabled
                        if settings.get("auto_reply_system", False) and settings.get("auto_reply_message"):
                            auto_reply_message = settings.get("auto_reply_message", "")
                            try:
                                # Send auto reply
                                chat_entity = await client.get_input_entity(chat_id)
                                await client.send_message(
                                    chat_entity,
                                    auto_reply_message,
                                    reply_to=message_id
                                )
                                logger.info(f"Auto reply sent for duplicate in chat {chat_id}")
                            except Exception as e:
                                logger.exception(f"Error sending auto reply: {e}")
                        
                        # Send notification if manual reply is enabled
                        if settings.get("manual_reply_system", True):
                            try:
                                if notification_queue:
                                    await notification_queue.put((user_id, task, chat_id, message_id, message_text, message_hash))
                                else:
                                    logger.error("Notification queue not initialized!")
                            except asyncio.QueueFull:
                                logger.warning("Notification queue full, dropping duplicate alert for user=%s", user_id)
                            except Exception as e:
                                logger.exception(f"Error queuing notification: {e}")
                        continue
                    
                    # Store message hash for future duplicate detection
                    store_message_hash(user_id, chat_id, message_hash, message_text)
                    
        except Exception as e:
            logger.exception(f"Error in monitor message handler for user {user_id}, chat {chat_id}: {e}")

    try:
        # Register handler for this specific chat only
        client.add_event_handler(_monitor_chat_handler, events.NewMessage(chats=chat_id))
        client.add_event_handler(_monitor_chat_handler, events.MessageEdited(chats=chat_id))
        
        # Store handler reference
        handler_registered.setdefault(user_id, []).append(_monitor_chat_handler)
        logger.info(f"Registered handler for user {user_id}, chat {chat_id}")
    except Exception as e:
        logger.exception(f"Failed to register handler for user {user_id}, chat {chat_id}: {e}")


async def start_monitoring_for_user(user_id: int):
    """Start monitoring for a user"""
    if user_id not in user_clients:
        logger.warning(f"User {user_id} not in user_clients")
        return

    client = user_clients[user_id]
    tasks_cache.setdefault(user_id, [])
    chat_entity_cache.setdefault(user_id, {})

    # Load user tasks if not already loaded
    if not tasks_cache.get(user_id):
        try:
            user_tasks = await db_call(db.get_user_tasks, user_id)
            tasks_cache[user_id] = user_tasks
            logger.info(f"Loaded {len(user_tasks)} tasks for user {user_id}")
        except Exception as e:
            logger.exception(f"Error loading tasks for user {user_id}: {e}")
    
    # Set up handlers for monitored chats
    await update_monitoring_for_user(user_id)


async def notification_worker(worker_id: int):
    """Worker that sends duplicate notifications"""
    logger.info(f"Notification worker {worker_id} started")
    global notification_queue, BOT_INSTANCE
    
    if notification_queue is None:
        logger.error("notification_worker started before queue initialized")
        return
    
    if BOT_INSTANCE is None:
        logger.error("Bot instance not available for notification worker")
        return

    while True:
        try:
            user_id, task, chat_id, message_id, message_text, message_hash = await notification_queue.get()
            logger.info(f"Processing notification for user {user_id}, chat {chat_id}")
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.exception(f"Error getting item from notification_queue in worker {worker_id}: {e}")
            break

        try:
            # Check if manual reply system is enabled
            settings = task.get("settings", {})
            if not settings.get("manual_reply_system", True):
                logger.debug(f"Manual reply system disabled for user {user_id}")
                continue
            
            # Prepare notification message
            task_label = task.get("label", "Unknown")
            preview_text = message_text[:100] + "..." if len(message_text) > 100 else message_text
            
            notification_msg = (
                f"🚨 **DUPLICATE MESSAGE DETECTED!**\n\n"
                f"**Task:** {task_label}\n"
                f"**Time:** {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n"
                f"📝 **Message Preview:**\n`{preview_text}`\n\n"
                f"💬 **Reply to this message to respond to the duplicate!**\n"
                f"(Swipe left on this message and type your reply)"
            )
            
            try:
                # Send notification via bot
                sent_message = await BOT_INSTANCE.send_message(
                    chat_id=user_id,
                    text=notification_msg,
                    parse_mode="Markdown"
                )
                
                # Store notification message for reply tracking
                notification_messages[sent_message.message_id] = {
                    "user_id": user_id,
                    "task_label": task_label,
                    "chat_id": chat_id,
                    "original_message_id": message_id,
                    "duplicate_hash": message_hash,
                    "message_preview": preview_text
                }
                
                logger.info(f"✅ Sent duplicate notification to user {user_id} for chat {chat_id}")
                
            except Exception as e:
                logger.error(f"Failed to send notification to user {user_id}: {e}")
                
        except Exception as e:
            logger.exception(f"Unexpected error in notification worker {worker_id}: {e}")
        finally:
            try:
                notification_queue.task_done()
            except Exception:
                pass


async def start_workers(bot):
    """Start all worker threads"""
    global _workers_started, notification_queue, worker_tasks, BOT_INSTANCE
    
    if _workers_started:
        return
    
    BOT_INSTANCE = bot
    notification_queue = asyncio.Queue(maxsize=SEND_QUEUE_MAXSIZE)

    # Start notification workers
    for i in range(MONITOR_WORKER_COUNT):
        t = asyncio.create_task(notification_worker(i + 1))
        worker_tasks.append(t)

    _workers_started = True
    logger.info(f"✅ Spawned {MONITOR_WORKER_COUNT} monitoring workers")


# ---------- Session restore ----------
async def restore_sessions():
    logger.info("🔄 Restoring sessions...")

    # First, restore from environment variable sessions
    logger.info(f"Checking {len(USER_SESSIONS)} saved sessions from environment...")
    for user_id_str, session_string in USER_SESSIONS.items():
        try:
            user_id = int(user_id_str)
            
            # Skip if already connected
            if user_id in user_clients:
                logger.debug(f"User {user_id} already connected, skipping")
                continue
                
            logger.info(f"Attempting to restore session for user {user_id} from env var")
            
            client = TelegramClient(
                StringSession(session_string),
                API_ID,
                API_HASH,
                device_model="Duplicate Monitor Bot",
                system_version="1.0",
                app_version="1.0",
                lang_code="en"
            )
            
            try:
                await client.connect()
                
                if await client.is_user_authorized():
                    user_clients[user_id] = client
                    chat_entity_cache.setdefault(user_id, {})
                    
                    # Get user info
                    me = await client.get_me()
                    
                    # Update database
                    await db_call(db.save_user, user_id, me.phone, me.first_name, session_string, True)
                    
                    # Load tasks
                    tasks_cache.setdefault(user_id, [])
                    if not tasks_cache.get(user_id):
                        try:
                            user_tasks = await db_call(db.get_user_tasks, user_id)
                            tasks_cache[user_id] = user_tasks
                            logger.info(f"Loaded {len(user_tasks)} tasks for user {user_id}")
                        except Exception as e:
                            logger.exception(f"Error loading tasks for user {user_id}: {e}")
                    
                    # Start monitoring
                    await start_monitoring_for_user(user_id)
                    
                    logger.info(f"✅ Restored session for user {user_id} ({me.first_name}) from environment")
                else:
                    logger.warning(f"Session expired for user {user_id}, removing from env var")
                    # Remove expired session
                    USER_SESSIONS.pop(user_id_str, None)
                    await db_call(db.save_user, user_id, None, None, None, False)
                    
            except Exception as e:
                logger.error(f"Failed to restore session for user {user_id}: {e}")
                USER_SESSIONS.pop(user_id_str, None)
                
        except ValueError:
            logger.warning(f"Invalid user ID in USER_SESSIONS: {user_id_str}")
        except Exception as e:
            logger.exception(f"Error processing session for user {user_id_str}: {e}")

    # Then restore from database (for backward compatibility)
    def _fetch_logged_in_users():
        conn = db.get_connection()
        cur = conn.cursor()
        cur.execute("SELECT user_id, session_data FROM users WHERE is_logged_in = 1")
        return cur.fetchall()

    try:
        users = await asyncio.to_thread(_fetch_logged_in_users)
    except Exception:
        logger.exception("Error fetching logged-in users from DB")
        users = []

    try:
        all_active = await db_call(db.get_all_active_tasks)
    except Exception:
        logger.exception("Error fetching active tasks from DB")
        all_active = []

    # Update tasks cache
    for t in all_active:
        uid = t["user_id"]
        tasks_cache.setdefault(uid, [])
        tasks_cache[uid].append({
            "id": t["id"], 
            "label": t["label"], 
            "chat_ids": t["chat_ids"], 
            "is_active": 1,
            "settings": t.get("settings", {})
        })

    logger.info(f"📊 Found {len(users)} logged in user(s) in database")

    batch_size = 3
    for i in range(0, len(users), batch_size):
        batch = users[i:i + batch_size]
        restore_tasks = []
        
        for row in batch:
            try:
                user_id, session_data = row[0], row[1]
            except Exception:
                continue

            # Skip if already restored from env var
            if user_id in user_clients:
                continue
                
            if session_data:
                restore_tasks.append(restore_single_session_from_db(user_id, session_data))
        
        if restore_tasks:
            results = await asyncio.gather(*restore_tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    logger.error(f"Error restoring session from DB: {result}")
            await asyncio.sleep(2)


async def restore_single_session_from_db(user_id: int, session_data: str):
    """Restore a single user session from database"""
    try:
        logger.info(f"Restoring session for user {user_id} from database")
        client = TelegramClient(
            StringSession(session_data),
            API_ID,
            API_HASH,
            device_model="Duplicate Monitor Bot",
            system_version="1.0",
            app_version="1.0",
            lang_code="en"
        )
        await client.connect()

        if await client.is_user_authorized():
            user_clients[user_id] = client
            chat_entity_cache.setdefault(user_id, {})
            await start_monitoring_for_user(user_id)
            logger.info(f"✅ Restored session for user {user_id} from database")
            
            # Also save to environment variable system
            me = await client.get_me()
            USER_SESSIONS[str(user_id)] = session_data
            
            # Notify owners about restored session
            if OWNER_IDS:
                for owner_id in OWNER_IDS:
                    try:
                        await BOT_INSTANCE.send_message(
                            owner_id,
                            f"🔄 **Session Restored from Database**\n\n"
                            f"👤 **User ID:** `{user_id}`\n"
                            f"📱 **Phone:** `{me.phone}`\n"
                            f"👤 **Name:** `{me.first_name}`\n\n"
                            f"This session should be added to USER_SESSIONS env var for persistence.",
                            parse_mode="Markdown"
                        )
                    except Exception:
                        pass
        else:
            await db_call(db.save_user, user_id, None, None, None, False)
            logger.warning(f"⚠️ Session expired for user {user_id} in database")
    except Exception as e:
        logger.exception(f"❌ Failed to restore session for user {user_id} from database: {e}")
        try:
            await db_call(db.save_user, user_id, None, None, None, False)
        except Exception:
            logger.exception("Error marking user logged out after failed restore for %s", user_id)


# ---------- Admin commands ----------
async def adduser_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin-only: add a user (optionally as admin)."""
    user_id = update.effective_user.id

    if not await check_authorization(update, context):
        return

    is_admin_caller = await db_call(db.is_user_admin, user_id)
    if not is_admin_caller:
        await update.message.reply_text("❌ **Admin Only**\n\nThis command is only available to admins.", parse_mode="Markdown")
        return

    text = update.message.text.strip()
    parts = text.split()

    if len(parts) < 2:
        await update.message.reply_text(
            "❌ **Invalid format!**\n\n"
            "**Usage:**\n"
            "/adduser [USER_ID] - Add regular user\n"
            "/adduser [USER_ID] admin - Add admin user",
            parse_mode="Markdown",
        )
        return

    try:
        new_user_id = int(parts[1])
        is_admin = len(parts) > 2 and parts[2].lower() == "admin"

        added = await db_call(db.add_allowed_user, new_user_id, None, is_admin, user_id)
        if added:
            role = "👑 Admin" if is_admin else "👤 User"
            await update.message.reply_text(
                f"✅ **User added!**\n\nID: `{new_user_id}`\nRole: {role}",
                parse_mode="Markdown",
            )
            try:
                await context.bot.send_message(new_user_id, "✅ You have been added. Send /start to begin.", parse_mode="Markdown")
            except Exception:
                logger.exception("Could not notify new allowed user %s", new_user_id)
        else:
            await update.message.reply_text(f"❌ **User `{new_user_id}` already exists!**", parse_mode="Markdown")
    except ValueError:
        await update.message.reply_text("❌ **Invalid user ID!**\n\nUser ID must be a number.", parse_mode="Markdown")


async def removeuser_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin-only: remove a user and stop their monitoring permanently."""
    user_id = update.effective_user.id

    if not await check_authorization(update, context):
        return

    is_admin_caller = await db_call(db.is_user_admin, user_id)
    if not is_admin_caller:
        await update.message.reply_text("❌ **Admin Only**\n\nThis command is only available to admins.", parse_mode="Markdown")
        return

    text = update.message.text.strip()
    parts = text.split()

    if len(parts) < 2:
        await update.message.reply_text("❌ **Invalid format!**\n\n**Usage:** `/removeuser [USER_ID]`", parse_mode="Markdown")
        return

    try:
        remove_user_id = int(parts[1])

        removed = await db_call(db.remove_allowed_user, remove_user_id)
        if removed:
            if remove_user_id in user_clients:
                try:
                    client = user_clients[remove_user_id]
                    # Remove all handlers
                    if remove_user_id in handler_registered:
                        for handler in handler_registered[remove_user_id]:
                            try:
                                client.remove_event_handler(handler)
                            except Exception:
                                logger.exception("Error removing event handler for removed user %s", remove_user_id)
                        handler_registered.pop(remove_user_id, None)

                    await client.disconnect()
                except Exception:
                    logger.exception("Error disconnecting client for removed user %s", remove_user_id)
                finally:
                    user_clients.pop(remove_user_id, None)

            try:
                await db_call(db.save_user, remove_user_id, None, None, None, False)
            except Exception:
                logger.exception("Error saving user logged_out state for %s", remove_user_id)

            # Remove from environment variable system
            if str(remove_user_id) in USER_SESSIONS:
                del USER_SESSIONS[str(remove_user_id)]
                
            tasks_cache.pop(remove_user_id, None)
            chat_entity_cache.pop(remove_user_id, None)
            reply_states.pop(remove_user_id, None)
            auto_reply_states.pop(remove_user_id, None)

            await update.message.reply_text(f"✅ **User `{remove_user_id}` removed!**", parse_mode="Markdown")

            try:
                await context.bot.send_message(remove_user_id, "❌ You have been removed. Contact the owner to regain access.", parse_mode="Markdown")
            except Exception:
                logger.exception("Could not notify removed user %s", remove_user_id)
        else:
            await update.message.reply_text(f"❌ **User `{remove_user_id}` not found!**", parse_mode="Markdown")
    except ValueError:
        await update.message.reply_text("❌ **Invalid user ID!**\n\nUser ID must be a number.", parse_mode="Markdown")


async def listusers_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin-only: list allowed users."""
    user_id = update.effective_user.id

    if not await check_authorization(update, context):
        return

    is_admin_caller = await db_call(db.is_user_admin, user_id)
    if not is_admin_caller:
        await update.message.reply_text("❌ **Admin Only**\n\nThis command is only available to admins.", parse_mode="Markdown")
        return

    users = await db_call(db.get_all_allowed_users)

    if not users:
        await update.message.reply_text("📋 **No Allowed Users**\n\nThe allowed users list is empty.", parse_mode="Markdown")
        return

    user_list = "👥 **Allowed Users**\n\n"
    user_list += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"

    for i, user in enumerate(users, 1):
        role_emoji = "👑" if user["is_admin"] else "👤"
        role_text = "Admin" if user["is_admin"] else "User"
        username = user["username"] if user["username"] else "Unknown"

        user_list += f"{i}. {role_emoji} **{role_text}**\n"
        user_list += f"   ID: `{user['user_id']}`\n"
        if user["username"]:
            user_list += f"   Username: {username}\n"
        user_list += "\n"

    user_list += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    user_list += f"Total: **{len(users)} user(s)**"

    await update.message.reply_text(user_list, parse_mode="Markdown")


# ---------- Test command for debugging ----------
async def test_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Test command to check if bot is working"""
    user_id = update.effective_user.id
    
    # Test notification by simulating a duplicate
    if user_id in user_clients and user_id in tasks_cache and len(tasks_cache[user_id]) > 0:
        # Create a test notification
        if notification_queue:
            task = tasks_cache[user_id][0]
            test_hash = hashlib.sha256(f"test_{time.time()}".encode()).hexdigest()[:16]
            await notification_queue.put((user_id, task, -1000000000, 999, "This is a test duplicate message!", test_hash))
            
            await update.message.reply_text(
                f"🧪 **Test Notification Sent!**\n\n"
                f"✅ A test notification has been queued.\n"
                f"📋 You should receive it in a few seconds.\n"
                f"💬 You can reply to it (swipe left) to test the reply system.\n\n"
                f"📊 Stats:\n"
                f"• Tasks: {len(tasks_cache.get(user_id, []))}\n"
                f"• Queue size: {notification_queue.qsize()}\n"
                f"• Connected: {'✅' if user_id in user_clients else '❌'}",
                parse_mode="Markdown"
            )
        else:
            await update.message.reply_text(
                f"⚠️ **Cannot Send Test**\n\n"
                f"Queue: {'❌ Not initialized'}\n"
                f"Tasks: {len(tasks_cache.get(user_id, []))}\n"
                f"Connected: {'✅' if user_id in user_clients else '❌'}",
                parse_mode="Markdown"
            )
    else:
        await update.message.reply_text(
            f"🤖 **Bot Test**\n\n"
            f"✅ Bot is running!\n"
            f"👤 User ID: `{user_id}`\n"
            f"🔗 Connected: {'✅' if user_id in user_clients else '❌'}\n"
            f"📋 Tasks: {len(tasks_cache.get(user_id, []))}\n\n"
            f"💡 Create a monitoring task first with /monitoradd",
            parse_mode="Markdown"
        )


# ---------- Graceful shutdown cleanup ----------
async def shutdown_cleanup():
    """Disconnect Telethon clients and cancel worker tasks cleanly."""
    logger.info("Shutdown cleanup: cancelling worker tasks and disconnecting clients...")

    for t in list(worker_tasks):
        try:
            t.cancel()
        except Exception:
            logger.exception("Error cancelling worker task")
    if worker_tasks:
        try:
            await asyncio.gather(*worker_tasks, return_exceptions=True)
        except Exception:
            logger.exception("Error while awaiting worker task cancellations")

    user_ids = list(user_clients.keys())
    batch_size = 5
    for i in range(0, len(user_ids), batch_size):
        batch = user_ids[i:i + batch_size]
        disconnect_tasks = []
        for uid in batch:
            client = user_clients.get(uid)
            if client:
                # Remove all handlers
                if uid in handler_registered:
                    for handler in handler_registered[uid]:
                        try:
                            client.remove_event_handler(handler)
                        except Exception:
                            logger.exception("Error removing event handler during shutdown for user %s", uid)
                    handler_registered.pop(uid, None)

                disconnect_tasks.append(client.disconnect())
        
        if disconnect_tasks:
            await asyncio.gather(*disconnect_tasks, return_exceptions=True)
    
    user_clients.clear()

    try:
        db.close_connection()
    except Exception:
        logger.exception("Error closing DB connection during shutdown")

    logger.info("Shutdown cleanup complete.")


# ---------- Application post_init ----------
async def post_init(application: Application):
    global MAIN_LOOP, BOT_INSTANCE
    MAIN_LOOP = asyncio.get_running_loop()
    BOT_INSTANCE = application.bot

    logger.info("🔧 Initializing bot...")

    await application.bot.delete_webhook(drop_pending_updates=True)
    logger.info("🧹 Cleared webhooks")

    if OWNER_IDS:
        for oid in OWNER_IDS:
            try:
                is_admin = await db_call(db.is_user_admin, oid)
                if not is_admin:
                    await db_call(db.add_allowed_user, oid, None, True, None)
                    logger.info("✅ Added owner/admin from env: %s", oid)
            except Exception:
                logger.exception("Error adding owner/admin %s from env", oid)

    if ALLOWED_USERS:
        for au in ALLOWED_USERS:
            try:
                await db_call(db.add_allowed_user, au, None, False, None)
                logger.info("✅ Added allowed user from env: %s", au)
            except Exception:
                logger.exception("Error adding allowed user %s from env: %s", au)

    await start_workers(application.bot)
    await restore_sessions()

    async def _collect_metrics():
        try:
            nq = notification_queue.qsize() if notification_queue is not None else None
            
            return {
                "notification_queue_size": nq,
                "worker_count": len(worker_tasks),
                "active_user_clients_count": len(user_clients),
                "monitoring_tasks_counts": {uid: len(tasks_cache.get(uid, [])) for uid in list(tasks_cache.keys())},
                "message_history_size": sum(len(v) for v in message_history.values()),
                "memory_usage_mb": _get_memory_usage_mb(),
                "duplicate_window_seconds": DUPLICATE_CHECK_WINDOW,
                "max_users": MAX_CONCURRENT_USERS,
                "saved_sessions_count": len(USER_SESSIONS),
            }
        except Exception as e:
            return {"error": f"failed to collect metrics in loop: {e}"}

    def _forward_metrics():
        global MAIN_LOOP
        if MAIN_LOOP is not None:
            try:
                future = asyncio.run_coroutine_threadsafe(_collect_metrics(), MAIN_LOOP)
                return future.result(timeout=1.0)
            except Exception as e:
                logger.exception("Failed to collect metrics from main loop")
                return {"error": f"failed to collect metrics: {e}"}
        else:
            return {"error": "bot main loop not available"}

    try:
        register_monitoring(_forward_metrics)
    except Exception:
        logger.exception("Failed to register monitoring callback with webserver")

    logger.info("✅ Bot initialized!")


def _get_memory_usage_mb():
    """Get current memory usage in MB"""
    try:
        import psutil
        process = psutil.Process()
        return round(process.memory_info().rss / 1024 / 1024, 2)
    except ImportError:
        return None


# ---------- Main -----------
def main():
    global BOT_INSTANCE
    
    if not BOT_TOKEN:
        logger.error("❌ BOT_TOKEN not found")
        return

    if not API_ID or not API_HASH:
        logger.error("❌ API_ID or API_HASH not found")
        return

    logger.info(f"🤖 Starting Duplicate Monitor Bot (Max Users: {MAX_CONCURRENT_USERS}, Duplicate Window: {DUPLICATE_CHECK_WINDOW}s)...")

    start_server_thread()

    application = Application.builder().token(BOT_TOKEN).post_init(post_init).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("login", login_command))
    application.add_handler(CommandHandler("logout", logout_command))
    application.add_handler(CommandHandler("monitoradd", monitoradd_command))
    application.add_handler(CommandHandler("monitortasks", monitortasks_command))
    application.add_handler(CommandHandler("getallid", getallid_command))
    application.add_handler(CommandHandler("adduser", adduser_command))
    application.add_handler(CommandHandler("removeuser", removeuser_command))
    application.add_handler(CommandHandler("listusers", listusers_command))
    application.add_handler(CommandHandler("getstrings", getstrings_command))
    application.add_handler(CommandHandler("test", test_command))
    application.add_handler(CallbackQueryHandler(button_handler))
    
    # Message handlers in order of priority
    application.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND, 
        handle_notification_reply
    ), group=0)
    
    application.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND, 
        handle_auto_reply_message
    ), group=1)
    
    application.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND, 
        handle_login_process
    ), group=2)

    logger.info("✅ Bot ready!")
    try:
        application.run_polling(drop_pending_updates=True)
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.exception(f"Bot crashed: {e}")
    finally:
        try:
            asyncio.run(shutdown_cleanup())
        except Exception as e:
            logger.exception(f"Error during shutdown cleanup: {e}")


if __name__ == "__main__":
    main()
