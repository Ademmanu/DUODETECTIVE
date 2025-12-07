#!/usr/bin/env python3
import os
import asyncio
import logging
from typing import Dict, List, Optional
from datetime import datetime

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters
)

from database import Database

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger("notifier_bot")

# Environment variables
BOT_TOKEN = os.getenv("NOTIFIER_BOT_TOKEN", "")
if not BOT_TOKEN:
    BOT_TOKEN = os.getenv("BOT_TOKEN", "")  # Fallback to main bot token

# Owner/admin configuration
OWNER_IDS: List[int] = []
owner_env = os.getenv("OWNER_IDS", "").strip()
if owner_env:
    for part in owner_env.split(","):
        part = part.strip()
        if part.isdigit():
            OWNER_IDS.append(int(part))

ALLOWED_USERS: List[int] = []
allowed_env = os.getenv("ALLOWED_USERS", "").strip()
if allowed_env:
    for part in allowed_env.split(","):
        part = part.strip()
        if part.isdigit():
            ALLOWED_USERS.append(int(part))

# Initialize database
db = Database("duplicate_monitor.db")

# User states
user_states: Dict[int, Dict] = {}  # user_id -> state data

UNAUTHORIZED_MESSAGE = """🚫 **Access Denied!**

You are not authorized to use this bot.

Please contact the administrator for access.
"""


# ========== Authorization ==========
async def check_authorization(update: Update) -> bool:
    """Check if user is authorized"""
    user_id = update.effective_user.id
    
    # Check environment variables first
    if user_id in OWNER_IDS or user_id in ALLOWED_USERS:
        return True
    
    # Check database
    try:
        if db.is_user_allowed(user_id):
            return True
    except Exception as e:
        logger.error(f"Error checking authorization: {e}")
    
    return False


async def require_authorization(func):
    """Decorator for authorization check"""
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await check_authorization(update):
            await update.message.reply_text(
                UNAUTHORIZED_MESSAGE,
                parse_mode="Markdown"
            )
            return
        return await func(update, context)
    return wrapper


# ========== Command Handlers ==========
@require_authorization
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start command handler"""
    user_id = update.effective_user.id
    user_name = update.effective_user.first_name or "User"
    
    welcome_message = f"""
🤖 **Duplicate Message Monitor Bot**

Hello {user_name}!

I will notify you when duplicate messages are detected in your monitored chats.

**Commands:**
/start - Show this menu
/notifications - View pending notifications
/tasks - View your monitor tasks
/stats - View statistics
/help - Show help

**How it works:**
1. I monitor your selected Telegram chats
2. When a duplicate message is detected
3. I notify you immediately
4. You reply with your response
5. Your response is sent to the original chat

Ready to monitor duplicates! 🚀
    """
    
    keyboard = [
        [InlineKeyboardButton("📋 My Tasks", callback_data="view_tasks")],
        [InlineKeyboardButton("🔔 Notifications", callback_data="view_notifications")]
    ]
    
    await update.message.reply_text(
        welcome_message,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown"
    )


@require_authorization
async def notifications_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """View pending notifications"""
    user_id = update.effective_user.id
    
    # Get user's tasks
    tasks = db.get_user_tasks(user_id)
    if not tasks:
        await update.message.reply_text(
            "📭 **No tasks found!**\n\n"
            "You don't have any monitor tasks yet.\n"
            "Use the monitor bot to create tasks.",
            parse_mode="Markdown"
        )
        return
    
    # Get pending notifications for all tasks
    all_notifications = []
    for task in tasks:
        notifications = db.get_pending_notifications_by_task(task['id'], status='notified')
        for notif in notifications:
            notif['task_label'] = task['label']
            all_notifications.append(notif)
    
    if not all_notifications:
        await update.message.reply_text(
            "✅ **No pending notifications!**\n\n"
            "All caught up! No duplicate messages need your attention.",
            parse_mode="Markdown"
        )
        return
    
    # Display notifications
    message = "🔔 **Pending Notifications**\n\n"
    message += f"Found **{len(all_notifications)}** notifications:\n\n"
    
    keyboard = []
    
    for i, notif in enumerate(all_notifications[:10], 1):  # Limit to 10
        task_label = notif['task_label']
        message_text = notif['message_text']
        truncated = message_text[:50] + "..." if len(message_text) > 50 else message_text
        
        message += f"{i}. **{task_label}**\n"
        message += f"   Message: `{truncated}`\n"
        message += f"   [Reply Now](https://t.me/{context.bot.username}?start=reply_{notif['id']})\n\n"
        
        keyboard.append([
            InlineKeyboardButton(
                f"Reply to #{notif['id']}",
                callback_data=f"reply_{notif['id']}"
            )
        ])
    
    if len(all_notifications) > 10:
        message += f"... and {len(all_notifications) - 10} more notifications\n"
    
    await update.message.reply_text(
        message,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="Markdown",
        disable_web_page_preview=True
    )


@require_authorization
async def tasks_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """View user's monitor tasks"""
    user_id = update.effective_user.id
    
    tasks = db.get_user_tasks(user_id)
    if not tasks:
        await update.message.reply_text(
            "📋 **No Tasks Found**\n\n"
            "You don't have any monitor tasks yet.\n"
            "Use the monitor bot to create tasks.",
            parse_mode="Markdown"
        )
        return
    
    message = "📋 **Your Monitor Tasks**\n\n"
    
    for i, task in enumerate(tasks, 1):
        status = "🟢 Active" if task.get('is_active', True) else "🔴 Inactive"
        chats = task.get('monitored_chat_ids', [])
        
        message += f"{i}. **{task['label']}** {status}\n"
        message += f"   Chats: {len(chats)} monitored\n"
        message += f"   Method: {task.get('detection_method', 'hash')}\n"
        message += f"   Window: {task.get('duplicate_window_hours', 24)} hours\n\n"
    
    message += f"Total: **{len(tasks)} task(s)**\n"
    
    # Get statistics
    stats = db.get_task_stats(tasks[0]['id']) if tasks else {}
    if stats:
        message += f"Duplicates found: **{stats.get('duplicates_found', 0)}**\n"
        message += f"Pending notifications: **{stats.get('pending_notifications', 0)}**"
    
    await update.message.reply_text(
        message,
        parse_mode="Markdown"
    )


@require_authorization
async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show user statistics"""
    user_id = update.effective_user.id
    
    tasks = db.get_user_tasks(user_id)
    total_duplicates = 0
    total_pending = 0
    
    for task in tasks:
        stats = db.get_task_stats(task['id'])
        total_duplicates += stats.get('duplicates_found', 0)
        total_pending += stats.get('pending_notifications', 0)
    
    stats_message = f"""
📊 **Statistics for {update.effective_user.first_name}**

**Tasks:**
• Total Tasks: {len(tasks)}
• Active Tasks: {sum(1 for t in tasks if t.get('is_active', True))}

**Detection:**
• Total Duplicates Found: {total_duplicates}
• Pending Notifications: {total_pending}

**Monitoring:**
• Total Chats Monitored: {sum(len(t.get('monitored_chat_ids', [])) for t in tasks)}
• Detection Window: 1-24 hours (configurable per task)

Keep up the good monitoring! 🎯
    """
    
    await update.message.reply_text(
        stats_message,
        parse_mode="Markdown"
    )


@require_authorization
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Help command"""
    help_text = """
🤖 **Duplicate Message Monitor Bot - Help**

**Commands:**
/start - Start the bot
/notifications - View pending notifications
/tasks - View your monitor tasks
/stats - View statistics
/help - This help message

**How to use:**

1. **Setup Monitoring:**
   • Use the monitor bot to log into your Telegram account
   • Create monitor tasks for specific chats
   • Set your duplicate detection preferences

2. **Receive Notifications:**
   • When duplicates are detected, you'll get notified here
   • Each notification includes the message and context

3. **Reply to Duplicates:**
   • Click "Reply" on any notification
   • Type your response message
   • Your reply will be sent to the original chat

**Tips:**
• You can monitor multiple chats simultaneously
• Adjust detection sensitivity per task
• Set different reply strategies for different chats

Need help? Contact the administrator.
    """
    
    await update.message.reply_text(
        help_text,
        parse_mode="Markdown"
    )


# ========== Callback Handlers ==========
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle inline button clicks"""
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    
    if not await check_authorization(update):
        await query.edit_message_text(
            UNAUTHORIZED_MESSAGE,
            parse_mode="Markdown"
        )
        return
    
    data = query.data
    
    if data == "view_tasks":
        await tasks_command(update, context)
        await query.message.delete()
    
    elif data == "view_notifications":
        await notifications_command(update, context)
        await query.message.delete()
    
    elif data.startswith("reply_"):
        notification_id = int(data.replace("reply_", ""))
        
        # Get notification details
        notification = db.get_pending_notification(notification_id)
        if not notification:
            await query.message.reply_text(
                "❌ **Notification not found!**\n\n"
                "This notification may have been already handled.",
                parse_mode="Markdown"
            )
            return
        
        # Set user state for reply
        user_states[user_id] = {
            'waiting_reply': True,
            'notification_id': notification_id,
            'chat_id': notification['chat_id'],
            'message_id': notification['message_id']
        }
        
        # Show reply prompt
        message_text = notification['message_text']
        truncated = message_text[:200] + "..." if len(message_text) > 200 else message_text
        
        reply_prompt = f"""
✍️ **Reply to Duplicate Message**

**Original Message:** {truncated}
**Details:**
• Chat ID: `{notification['chat_id']}`
• Message ID: `{notification['message_id']}`
• Sender: {'@' + notification['sender_username'] if notification['sender_username'] else 'ID: ' + str(notification['sender_id'])}

**Please type your reply message below:**
(your reply will be sent to the original chat)
        """
        
        await query.message.reply_text(
            reply_prompt,
            parse_mode="Markdown"
        )


# ========== Message Handler for Replies ==========
async def handle_reply_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle user's reply to a notification"""
    user_id = update.effective_user.id
    
    if user_id not in user_states or not user_states[user_id].get('waiting_reply'):
        return
    
    state = user_states[user_id]
    reply_text = update.message.text.strip()
    
    if not reply_text:
        await update.message.reply_text(
            "❌ **Reply cannot be empty!**\n\n"
            "Please type a message to send as reply.",
            parse_mode="Markdown"
        )
        return
    
    # Get notification details
    notification_id = state['notification_id']
    notification = db.get_pending_notification(notification_id)
    
    if not notification:
        await update.message.reply_text(
            "❌ **Notification not found!**\n\n"
            "This notification may have been already handled.",
            parse_mode="Markdown"
        )
        user_states.pop(user_id, None)
        return
    
    # Update notification status
    db.update_notification_status(notification_id, 'replied', reply_text)
    
    # Send confirmation
    await update.message.reply_text(
        f"✅ **Reply queued for sending!**\n\n"
        f"**Your reply:** {reply_text[:100]}{'...' if len(reply_text) > 100 else ''}\n\n"
        f"The reply will be sent to the original chat shortly.\n"
        f"Notification ID: `{notification_id}`",
        parse_mode="Markdown"
    )
    
    # Clear user state
    user_states.pop(user_id, None)
    
    # Note: In a real implementation, you would send this to the monitor.py
    # to actually send the reply. This could be done via:
    # 1. Database queue (monitor.py periodically checks for new replies)
    # 2. Direct API call to monitor.py
    # 3. Message queue (Redis, RabbitMQ, etc.)
    
    logger.info(f"User {user_id} replied to notification {notification_id}: {reply_text[:50]}...")


# ========== Admin Commands ==========
async def adduser_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add an allowed user (admin only)"""
    user_id = update.effective_user.id
    
    # Check if user is admin
    if user_id not in OWNER_IDS and not db.is_user_admin(user_id):
        await update.message.reply_text(
            "❌ **Admin Only!**\n\n"
            "This command is only available to administrators.",
            parse_mode="Markdown"
        )
        return
    
    # Parse command arguments
    args = context.args
    if len(args) < 1:
        await update.message.reply_text(
            "❌ **Usage:** `/adduser <user_id> [admin]`\n\n"
            "Example: `/adduser 123456789 admin`",
            parse_mode="Markdown"
        )
        return
    
    try:
        new_user_id = int(args[0])
        is_admin = len(args) > 1 and args[1].lower() == 'admin'
        
        success = db.add_allowed_user(
            user_id=new_user_id,
            is_admin=is_admin,
            added_by=user_id
        )
        
        if success:
            role = "👑 Admin" if is_admin else "👤 User"
            await update.message.reply_text(
                f"✅ **User added!**\n\n"
                f"ID: `{new_user_id}`\n"
                f"Role: {role}",
                parse_mode="Markdown"
            )
        else:
            await update.message.reply_text(
                f"❌ **User `{new_user_id}` already exists!**",
                parse_mode="Markdown"
            )
            
    except ValueError:
        await update.message.reply_text(
            "❌ **Invalid user ID!**\n\n"
            "User ID must be a number.",
            parse_mode="Markdown"
        )


async def listusers_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List all allowed users (admin only)"""
    user_id = update.effective_user.id
    
    # Check if user is admin
    if user_id not in OWNER_IDS and not db.is_user_admin(user_id):
        await update.message.reply_text(
            "❌ **Admin Only!**\n\n"
            "This command is only available to administrators.",
            parse_mode="Markdown"
        )
        return
    
    users = db.get_all_allowed_users()
    
    if not users:
        await update.message.reply_text(
            "📋 **No Allowed Users**\n\n"
            "The allowed users list is empty.",
            parse_mode="Markdown"
        )
        return
    
    message = "👥 **Allowed Users**\n\n"
    
    for i, user in enumerate(users, 1):
        role = "👑 Admin" if user['is_admin'] else "👤 User"
        username = f"(@{user['username']})" if user['username'] else ""
        
        message += f"{i}. **ID:** `{user['user_id']}` {username}\n"
        message += f"   **Role:** {role}\n"
        message += f"   **Added:** {user['created_at'][:10]}\n\n"
    
    message += f"Total: **{len(users)} user(s)**"
    
    await update.message.reply_text(
        message,
        parse_mode="Markdown"
    )


# ========== Main Application ==========
def main():
    """Start the notifier bot"""
    if not BOT_TOKEN:
        logger.error("❌ BOT_TOKEN not found in environment variables!")
        return
    
    logger.info("🤖 Starting Notifier Bot...")
    
    # Create application
    application = Application.builder().token(BOT_TOKEN).build()
    
    # Add command handlers
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("notifications", notifications_command))
    application.add_handler(CommandHandler("tasks", tasks_command))
    application.add_handler(CommandHandler("stats", stats_command))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("adduser", adduser_command))
    application.add_handler(CommandHandler("listusers", listusers_command))
    
    # Add callback query handler
    application.add_handler(CallbackQueryHandler(button_handler))
    
    # Add message handler for replies
    application.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            handle_reply_message
        )
    )
    
    # Start the bot
    logger.info("✅ Notifier Bot is running...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
