#!/usr/bin/env python3
import os
import asyncio
import logging
import functools
import hashlib
import time
import gc
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
from database import Database
from webserver import start_server_thread, register_monitoring

# Optimized logging to reduce I/O
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("monitor")

# Environment variables with optimized defaults for Render free tier
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

# Tuning parameters for Render free tier
MONITOR_WORKER_COUNT = int(os.getenv("MONITOR_WORKER_COUNT", "10"))
SEND_QUEUE_MAXSIZE = int(os.getenv("SEND_QUEUE_MAXSIZE", "5000"))
DUPLICATE_CHECK_WINDOW = int(os.getenv("DUPLICATE_CHECK_WINDOW", "3600"))  # 1 hour
MAX_CONCURRENT_USERS = int(os.getenv("MAX_CONCURRENT_USERS", "50"))
MESSAGE_HASH_LIMIT = int(os.getenv("MESSAGE_HASH_LIMIT", "1000"))  # Store last 1000 messages

db = Database()

# Data structures
user_clients: Dict[int, TelegramClient] = {}
login_states: Dict[int, Dict] = {}
logout_states: Dict[int, Dict] = {}
reply_states: Dict[int, Dict] = {}  # user_id -> {waiting_reply_for: task_label, chat_id, message_id, duplicate_hash}

# Task creation states
task_creation_states: Dict[int, Dict[str, Any]] = {}

# Caches
tasks_cache: Dict[int, List[Dict]] = {}
chat_entity_cache: Dict[int, Dict[int, object]] = {}
handler_registered: Dict[int, Callable] = {}
message_history: Dict[Tuple[int, int], List[Tuple[str, float]]] = {}  # (user_id, chat_id) -> [(hash, timestamp)]

# Global queues
monitor_queue: Optional[asyncio.Queue] = None
notification_queue: Optional[asyncio.Queue] = None
send_queue: Optional[asyncio.Queue] = None

# Application object (module-level) so workers can use it without importing the module
application: Optional[Application] = None

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
    """Run garbage collection periodically to free memory"""
    global _last_gc_run
    current_time = asyncio.get_event_loop().time()
    if current_time - _last_gc_run > GC_INTERVAL:
        collected = gc.collect()
        logger.debug(f"Garbage collection freed {collected} objects")
        _last_gc_run = current_time


# ---------- Duplicate Detection ----------
def create_message_hash(message_text: str, sender_id: Optional[int] = None) -> str:
    """Create a hash for message duplicate detection"""
    content = message_text.strip().lower()
    if sender_id:
        content = f"{sender_id}:{content}"
    return hashlib.sha256(content.encode()).hexdigest()


def is_duplicate_message(user_id: int, chat_id: int, message_hash: str) -> bool:
    """Check if message is a duplicate within the time window"""
    key = (user_id, chat_id)
    if key not in message_history:
        return False
    
    current_time = time.time()
    # Clean old messages outside the window
    message_history[key] = [
        (h, t) for h, t in message_history[key]
        if current_time - t <= DUPLICATE_CHECK_WINDOW
    ]
    
    # Check for duplicate
    for stored_hash, _ in message_history[key]:
        if stored_hash == message_hash:
            return True
    
    return False


def store_message_hash(user_id: int, chat_id: int, message_hash: str):
    """Store message hash for duplicate checking"""
    key = (user_id, chat_id)
    if key not in message_history:
        message_history[key] = []
    
    # Add new hash
    message_history[key].append((message_hash, time.time()))
    
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
                    "duplicate_detection": True,
                    "notification_alerts": True,
                    "manual_reply_system": True,
                    "auto_reply_system": False,
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
                        "• Duplicate detection: ✅ Active\n"
                        "• Notification alerts: ✅ Enabled\n"
                        "• Manual reply system: ✅ Enabled\n"
                        "• Auto Reply system: ❌ Disabled\n"
                        "• Outgoing Message monitoring: ✅ Enabled\n\n"
                        "Use /monitortasks to manage your task!",
                        parse_mode="Markdown"
                    )

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
    
    duplicate_emoji = "✅" if settings.get("duplicate_detection", True) else "❌"
    notification_emoji = "✅" if settings.get("notification_alerts", True) else "❌"
    manual_reply_emoji = "✅" if settings.get("manual_reply_system", True) else "❌"
    auto_reply_emoji = "✅" if settings.get("auto_reply_system", False) else "❌"
    outgoing_emoji = "✅" if settings.get("outgoing_message_monitoring", True) else "❌"
    
    message_text = f"🔧 **Task Management: {task_label}**\n\n"
    message_text += f"📥 **Monitoring Chats:** {', '.join(map(str, task['chat_ids']))}\n\n"
    message_text += "⚙️ **Settings:**\n"
    message_text += f"{duplicate_emoji} Duplicate detection - Detects duplicate messages\n"
    message_text += f"{notification_emoji} Notification alerts - Sends alerts for duplicates\n"
    message_text += f"{manual_reply_emoji} Manual reply system - Allows manual replies\n"
    message_text += f"{auto_reply_emoji} Auto Reply system - Automatic replies (disabled by default)\n"
    message_text += f"{outgoing_emoji} Outgoing Message monitoring - Monitors your outgoing messages\n\n"
    message_text += "💡 **Tap any option below to change it!**"
    
    keyboard = [
        [
            InlineKeyboardButton(f"{duplicate_emoji} Duplicate", callback_data=f"toggle_{task_label}_duplicate_detection"),
            InlineKeyboardButton(f"{notification_emoji} Notify", callback_data=f"toggle_{task_label}_notification_alerts")
        ],
        [
            InlineKeyboardButton(f"{manual_reply_emoji} Manual Reply", callback_data=f"toggle_{task_label}_manual_reply_system"),
            InlineKeyboardButton(f"{auto_reply_emoji} Auto Reply", callback_data=f"toggle_{task_label}_auto_reply_system")
        ],
        [
            InlineKeyboardButton(f"{outgoing_emoji} Outgoing", callback_data=f"toggle_{task_label}_outgoing_message_monitoring"),
            InlineKeyboardButton("🗑️ Delete", callback_data=f"delete_{task_label}")
        ],
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
    if toggle_type == "duplicate_detection":
        new_state = not settings.get("duplicate_detection", True)
        settings["duplicate_detection"] = new_state
        status_text = "Duplicate detection"
        
    elif toggle_type == "notification_alerts":
        new_state = not settings.get("notification_alerts", True)
        settings["notification_alerts"] = new_state
        status_text = "Notification alerts"
        
    elif toggle_type == "manual_reply_system":
        new_state = not settings.get("manual_reply_system", True)
        settings["manual_reply_system"] = new_state
        status_text = "Manual reply system"
        
    elif toggle_type == "auto_reply_system":
        new_state = not settings.get("auto_reply_system", False)
        settings["auto_reply_system"] = new_state
        status_text = "Auto reply system"
        
    elif toggle_type == "outgoing_message_monitoring":
        new_state = not settings.get("outgoing_message_monitoring", True)
        settings["outgoing_message_monitoring"] = new_state
        status_text = "Outgoing message monitoring"
        
    else:
        await query.answer(f"Unknown toggle type: {toggle_type}")
        return
    
    # Update cache with new state
    task["settings"] = settings
    tasks_cache[user_id][task_index] = task
    
    # Update the button inline
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
        asyncio.create_task(
            db_call(db.update_task_settings, user_id, task_label, settings)
        )
    except Exception as e:
        logger.exception("Error updating task settings in DB: %s", e)


async def handle_reply_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle reply to duplicate notification"""
    query = update.callback_query
    user_id = query.from_user.id
    
    # Extract task_label from callback data
    # Format: reply_taskLabel_chatId_messageId_duplicateHash
    parts = query.data.replace("reply_", "").split("_")
    if len(parts) < 4:
        await query.answer("Invalid reply action!", show_alert=True)
        return
    
    task_label = parts[0]
    chat_id = int(parts[1])
    message_id = int(parts[2])
    duplicate_hash = parts[3]
    
    # Store reply state
    reply_states[user_id] = {
        "waiting_reply_for": task_label,
        "chat_id": chat_id,
        "message_id": message_id,
        "duplicate_hash": duplicate_hash,
        "notification_message_id": query.message.message_id
    }
    
    await query.answer()
    await query.edit_message_text(
        f"💬 **Ready for your reply!**\n\n"
        f"Task: **{task_label}**\n"
        f"Chat ID: `{chat_id}`\n"
        f"Duplicate Message ID: `{message_id}`\n\n"
        "**Type your reply now:**\n"
        "I'll send it to the original chat and reply to the duplicate message.\n\n"
        "💡 *You can include text, emojis, or any message content*",
        parse_mode="Markdown"
    )


async def handle_user_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle user's manual reply to duplicate"""
    user_id = update.effective_user.id
    text = update.message.text.strip()
    
    if user_id not in reply_states:
        return
    
    state = reply_states[user_id]
    task_label = state["waiting_reply_for"]
    chat_id = state["chat_id"]
    message_id = state["message_id"]
    duplicate_hash = state["duplicate_hash"]
    
    # Find the task
    user_tasks = tasks_cache.get(user_id, [])
    task = None
    for t in user_tasks:
        if t["label"] == task_label:
            task = t
            break
    
    if not task:
        await update.message.reply_text("❌ Task not found!")
        del reply_states[user_id]
        return
    
    # Check if user is logged in
    if user_id not in user_clients:
        await update.message.reply_text("❌ You need to be logged in to send replies!")
        del reply_states[user_id]
        return
    
    client = user_clients[user_id]
    
    try:
        # Get the chat entity
        chat_entity = await client.get_input_entity(chat_id)
        
        # Send the reply
        await client.send_message(
            chat_entity,
            text,
            reply_to=message_id
        )
        
        # Confirm to user
        await update.message.reply_text(
            f"✅ **Reply sent successfully!**\n\n"
            f"📝 **Your reply:** {text}\n"
            f"💬 **Sent to:** Chat `{chat_id}`\n"
            f"🔗 **Replying to message:** `{message_id}`\n\n"
            "The duplicate sender has been notified with your reply.",
            parse_mode="Markdown"
        )
        
        # Log the action
        logger.info(f"User {user_id} sent manual reply to duplicate in chat {chat_id}")
        
    except Exception as e:
        logger.exception(f"Error sending manual reply for user {user_id}: {e}")
        await update.message.reply_text(
            f"❌ **Failed to send reply:** {str(e)}\n\n"
            "Please try again or check your connection.",
            parse_mode="Markdown"
        )
    finally:
        # Clear reply state
        del reply_states[user_id]


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

    client = TelegramClient(StringSession(), API_ID, API_HASH)
    
    try:
        await client.connect()
    except Exception as e:
        logger.error(f"Telethon connection failed: {e}")
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
    
    # Check if we're waiting for manual reply
    if user_id in reply_states:
        return  # Let handle_user_reply handle this
    
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
                logger.info(f"Sending code request to {clean_phone}")
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

                await db_call(db.save_user, user_id, state["phone"], me.first_name, session_string, True)

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
                    "**Now you can:**\n"
                    "• Create monitoring tasks with /monitoradd\n"
                    "• View your tasks with /monitortasks\n"
                    "• Get chat IDs with /getallid\n\n"
                    "Welcome aboard! 🚀",
                    parse_mode="Markdown",
                )

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

                await db_call(db.save_user, user_id, state["phone"], me.first_name, session_string, True)

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
                    "**Now you can:**\n"
                    "• Create monitoring tasks with /monitoradd\n"
                    "• View your tasks with /monitortasks\n"
                    "• Get chat IDs with /getallid\n\n"
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
            handler = handler_registered.get(user_id)
            if handler:
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
    
    tasks_cache.pop(user_id, None)
    chat_entity_cache.pop(user_id, None)
    logout_states.pop(user_id, None)
    reply_states.pop(user_id, None)

    await update.message.reply_text(
        "👋 **Account disconnected successfully!**\n\n"
        "✅ All your monitoring tasks have been stopped.\n"
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
def ensure_handler_registered_for_user(user_id: int, client: TelegramClient):
    """Attach a NewMessage handler for monitoring"""
    if handler_registered.get(user_id):
        return

    async def _monitor_message_handler(event):
        try:
            await optimized_gc()
            
            message = getattr(event, "message", None)
            if not message:
                return
                
            message_text = getattr(event, "raw_text", None) or getattr(message, "message", None)
            if not message_text:
                return

            chat_id = getattr(event, "chat_id", None) or getattr(message, "chat_id", None)
            if chat_id is None:
                return

            sender_id = getattr(message, "sender_id", None)
            message_id = getattr(message, "id", None)
            message_outgoing = getattr(message, "out", False)

            user_tasks = tasks_cache.get(user_id)
            if not user_tasks:
                return

            for task in user_tasks:
                if chat_id not in task.get("chat_ids", []):
                    continue
                    
                settings = task.get("settings", {})
                
                # Check outgoing message monitoring
                if message_outgoing and not settings.get("outgoing_message_monitoring", True):
                    continue
                    
                # Check duplicate detection
                if settings.get("duplicate_detection", True):
                    message_hash = create_message_hash(message_text, sender_id)
                    
                    if is_duplicate_message(user_id, chat_id, message_hash):
                        # Duplicate found!
                        if settings.get("notification_alerts", True):
                            # Send notification
                            try:
                                await notification_queue.put((user_id, task, chat_id, message_id, message_text, message_hash))
                            except asyncio.QueueFull:
                                logger.warning("Notification queue full, dropping duplicate alert for user=%s", user_id)
                        continue
                    
                    # Store message hash for future duplicate detection
                    store_message_hash(user_id, chat_id, message_hash)
                    
        except Exception:
            logger.exception("Error in monitor message handler for user %s", user_id)

    try:
        client.add_event_handler(_monitor_message_handler, events.NewMessage())
        client.add_event_handler(_monitor_message_handler, events.MessageEdited())
        handler_registered[user_id] = _monitor_message_handler
        logger.info("Registered monitoring handler for user %s", user_id)
    except Exception:
        logger.exception("Failed to add event handler for user %s", user_id)


async def notification_worker(worker_id: int):
    """Worker that sends duplicate notifications"""
    logger.info("Notification worker %d started", worker_id)
    global notification_queue
    if notification_queue is None:
        logger.error("notification_worker started before queue initialized")
        return

    while True:
        try:
            user_id, task, chat_id, message_id, message_text, message_hash = await notification_queue.get()
        except asyncio.CancelledError:
            break
        except Exception:
            logger.exception("Error getting item from notification_queue in worker %d", worker_id)
            break

        try:
            # Check if manual reply system is enabled
            settings = task.get("settings", {})
            if not settings.get("manual_reply_system", True):
                continue
            
            # Prepare notification message
            task_label = task.get("label", "Unknown")
            preview_text = message_text[:100] + "..." if len(message_text) > 100 else message_text
            
            notification_msg = (
                f"🚨 **DUPLICATE MESSAGE DETECTED!**\n\n"
                f"**Task:** {task_label}\n"
                f"**Chat ID:** `{chat_id}`\n"
                f"**Message ID:** `{message_id}`\n"
                f"**Time:** {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n"
                f"📝 **Message Preview:**\n`{preview_text}`\n\n"
                f"💬 **Click below to send a manual reply:**"
            )
            
            # Create inline keyboard for reply
            keyboard = [[
                InlineKeyboardButton(
                    "💬 Reply to This Message",
                    callback_data=f"reply_{task_label}_{chat_id}_{message_id}_{message_hash}"
                )
            ]]
            
            # Send notification via bot using module-level application (no self-import)
            try:
                global application
                if application and application.bot:
                    await application.bot.send_message(
                        chat_id=user_id,
                        text=notification_msg,
                        reply_markup=InlineKeyboardMarkup(keyboard),
                        parse_mode="Markdown"
                    )
                    logger.info(f"Sent duplicate notification to user {user_id} for chat {chat_id}")
            except Exception as e:
                logger.exception(f"Error sending notification: {e}")
                
        except Exception:
            logger.exception("Unexpected error in notification worker %d", worker_id)
        finally:
            try:
                notification_queue.task_done()
            except Exception:
                pass


async def start_workers():
    """Start all worker threads"""
    global _workers_started, monitor_queue, notification_queue, send_queue, worker_tasks
    if _workers_started:
        return

    monitor_queue = asyncio.Queue(maxsize=SEND_QUEUE_MAXSIZE)
    notification_queue = asyncio.Queue(maxsize=SEND_QUEUE_MAXSIZE)
    send_queue = asyncio.Queue(maxsize=SEND_QUEUE_MAXSIZE)

    # Start notification workers
    for i in range(MONITOR_WORKER_COUNT):
        t = asyncio.create_task(notification_worker(i + 1))
        worker_tasks.append(t)

    _workers_started = True
    logger.info(f"Spawned {MONITOR_WORKER_COUNT} monitoring workers")


async def start_monitoring_for_user(user_id: int):
    """Start monitoring for a user"""
    if user_id not in user_clients:
        return

    client = user_clients[user_id]
    tasks_cache.setdefault(user_id, [])
    chat_entity_cache.setdefault(user_id, {})

    ensure_handler_registered_for_user(user_id, client)


# ---------- Session restore ----------
async def restore_sessions():
    logger.info("🔄 Restoring sessions...")

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

    tasks_cache.clear()
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

    logger.info("📊 Found %d logged in user(s)", len(users))

    batch_size = 5
    for i in range(0, len(users), batch_size):
        batch = users[i:i + batch_size]
        restore_tasks = []
        
        for row in batch:
            try:
                user_id = row["user_id"] if isinstance(row, dict) or hasattr(row, "keys") else row[0]
                session_data = row["session_data"] if isinstance(row, dict) or hasattr(row, "keys") else row[1]
            except Exception:
                try:
                    user_id, session_data = row[0], row[1]
                except Exception:
                    continue

            if session_data:
                restore_tasks.append(restore_single_session(user_id, session_data))
        
        if restore_tasks:
            await asyncio.gather(*restore_tasks, return_exceptions=True)
            await asyncio.sleep(1)


async def restore_single_session(user_id: int, session_data: str):
    """Restore a single user session"""
    try:
        client = TelegramClient(StringSession(session_data), API_ID, API_HASH)
        await client.connect()

        if await client.is_user_authorized():
            user_clients[user_id] = client
            chat_entity_cache.setdefault(user_id, {})
            await start_monitoring_for_user(user_id)
            logger.info("✅ Restored session for user %s", user_id)
        else:
            await db_call(db.save_user, user_id, None, None, None, False)
            logger.warning("⚠️ Session expired for user %s", user_id)
    except Exception as e:
        logger.exception("❌ Failed to restore session for user %s: %s", user_id, e)
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
                    handler = handler_registered.get(remove_user_id)
                    if handler:
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

            tasks_cache.pop(remove_user_id, None)
            chat_entity_cache.pop(remove_user_id, None)
            handler_registered.pop(remove_user_id, None)
            reply_states.pop(remove_user_id, None)

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
                handler = handler_registered.get(uid)
                if handler:
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
    global MAIN_LOOP
    MAIN_LOOP = asyncio.get_running_loop()

    # Ensure module-level application is set for workers
    try:
        globals()['application'] = application
    except Exception:
        logger.exception("Failed to set global application reference")

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

    await start_workers()
    await restore_sessions()

    async def _collect_metrics():
        try:
            nq = notification_queue.qsize() if notification_queue is not None else None
            sq = send_queue.qsize() if send_queue is not None else None
            mq = monitor_queue.qsize() if monitor_queue is not None else None
            
            return {
                "notification_queue_size": nq,
                "send_queue_size": sq,
                "monitor_queue_size": mq,
                "worker_count": len(worker_tasks),
                "active_user_clients_count": len(user_clients),
                "monitoring_tasks_counts": {uid: len(tasks_cache.get(uid, [])) for uid in list(tasks_cache.keys())},
                "message_history_size": sum(len(v) for v in message_history.values()),
                "memory_usage_mb": _get_memory_usage_mb(),
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
    global application
    if not BOT_TOKEN:
        logger.error("❌ BOT_TOKEN not found")
        return

    if not API_ID or not API_HASH:
        logger.error("❌ API_ID or API_HASH not found")
        return

    logger.info("🤖 Starting Duplicate Monitor Bot...")

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
    application.add_handler(CallbackQueryHandler(button_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_login_process))
    
    # Add handler for manual replies (must come after login handler)
    application.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND, 
        handle_user_reply
    ), group=1)

    logger.info("✅ Bot ready!")
    try:
        application.run_polling(drop_pending_updates=True)
    finally:
        try:
            asyncio.run(shutdown_cleanup())
        except Exception:
            logger.exception("Error during shutdown cleanup")


if __name__ == "__main__":
    main()
