# notifier_bot.py
#!/usr/bin/env python3
import logging
from datetime import datetime
from typing import Dict, Optional
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters
)
import config
from database import DuplicateMonitorDB

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("notifier-bot")

db = DuplicateMonitorDB()

class NotifierBot:
    def __init__(self):
        self.application = None
        
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /start command"""
        user_id = update.effective_user.id
        
        # Check if user is admin
        if user_id not in config.ADMIN_IDS:
            await update.message.reply_text(
                "🚫 **Access Denied!**\n\n"
                "This bot is only for administrators of the duplicate monitoring system.",
                parse_mode="Markdown"
            )
            return
        
        welcome_text = (
            "👋 **Duplicate Monitor Notification Bot**\n\n"
            "I will notify you when duplicate messages are detected in monitored chats.\n\n"
            "**Commands:**\n"
            "/stats - View monitoring statistics\n"
            "/pending - View pending replies\n"
            "/help - Show this help message\n\n"
            "When you receive a duplicate notification, simply reply to it with your response."
        )
        
        await update.message.reply_text(welcome_text, parse_mode="Markdown")
    
    async def stats_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show monitoring statistics"""
        user_id = update.effective_user.id
        
        if user_id not in config.ADMIN_IDS:
            return
        
        stats = db.get_stats(user_id)
        
        stats_text = (
            "📊 **Monitoring Statistics**\n\n"
            f"**Total Messages:** {stats.get('total_messages', 0)}\n"
            f"**Total Duplicates:** {stats.get('total_duplicates', 0)}\n"
            f"**Pending Replies:** {stats.get('pending_replies', 0)}\n\n"
        )
        
        # Get monitored chats
        chats = db.get_user_monitored_chats(user_id)
        if chats:
            stats_text += "**Monitored Chats:**\n"
            for chat in chats[:10]:  # Show first 10
                stats_text += f"• {chat['chat_name']} ({chat['chat_type']})\n"
            if len(chats) > 10:
                stats_text += f"... and {len(chats) - 10} more\n"
        
        await update.message.reply_text(stats_text, parse_mode="Markdown")
    
    async def pending_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show pending replies"""
        user_id = update.effective_user.id
        
        if user_id not in config.ADMIN_IDS:
            return
        
        # Get pending replies from database
        conn = db.get_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT d.id, d.message_text, d.detected_at, c.chat_name
            FROM duplicates d
            JOIN monitored_chats c ON d.chat_id = c.chat_id
            WHERE c.user_id = ? AND d.status = 'replied' AND d.reply_sent = 0
            ORDER BY d.detected_at DESC
            LIMIT 10
        """, (user_id,))
        
        pending = cur.fetchall()
        
        if not pending:
            await update.message.reply_text(
                "✅ **No pending replies!**\n\n"
                "All duplicate notifications have been handled.",
                parse_mode="Markdown"
            )
            return
        
        reply_text = "📝 **Pending Replies**\n\n"
        
        for i, row in enumerate(pending, 1):
            dup_id, message_text, detected_at, chat_name = row
            # Truncate long messages
            if len(message_text) > 50:
                display_text = message_text[:47] + "..."
            else:
                display_text = message_text
            
            reply_text += (
                f"{i}. **Chat:** {chat_name}\n"
                f"   **Message:** {display_text}\n"
                f"   **Detected:** {detected_at}\n"
                f"   **ID:** `{dup_id}`\n\n"
            )
        
        await update.message.reply_text(reply_text, parse_mode="Markdown")
    
    async def handle_duplicate_notification(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Handle incoming duplicate notifications from monitoring script.
        This would be called via a webhook or direct API call from monitor.py
        """
        # In actual implementation, this would receive data from monitor.py
        # For now, this is a placeholder
        pass
    
    async def handle_user_reply(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle user's reply to a duplicate notification"""
        user_id = update.effective_user.id
        
        if user_id not in config.ADMIN_IDS:
            return
        
        # Check if this is a reply to a bot message
        if not update.message.reply_to_message:
            await update.message.reply_text(
                "Please reply directly to a duplicate notification message.",
                parse_mode="Markdown"
            )
            return
        
        bot_message_id = update.message.reply_to_message.message_id
        reply_text = update.message.text.strip()
        
        if not reply_text:
            await update.message.reply_text(
                "Please include a reply message.",
                parse_mode="Markdown"
            )
            return
        
        # Find the conversation
        conversation = db.get_conversation_by_bot_message(bot_message_id)
        if not conversation:
            await update.message.reply_text(
                "❌ **Not a valid duplicate notification!**\n\n"
                "Please reply only to duplicate notification messages.",
                parse_mode="Markdown"
            )
            return
        
        # Save user's reply
        db.save_user_reply(conversation['id'], reply_text)
        
        # Send confirmation
        await update.message.reply_text(
            "✅ **Reply saved!**\n\n"
            f"Your reply has been recorded and will be sent to the chat.\n"
            f"**Duplicate ID:** `{conversation['duplicate_id']}`",
            parse_mode="Markdown"
        )
        
        # Here you would trigger the monitor to send the reply
        # In actual implementation, this could be via a queue, webhook, or direct call
        logger.info(f"User {user_id} replied to duplicate {conversation['duplicate_id']}")
    
    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show help message"""
        help_text = (
            "🤖 **Duplicate Monitor Bot Help**\n\n"
            "**How it works:**\n"
            "1. Monitoring script detects duplicate messages\n"
            "2. You receive notifications here\n"
            "3. Reply to notifications with your response\n"
            "4. Your reply is sent to the original chat\n\n"
            "**Commands:**\n"
            "/start - Start the bot\n"
            "/stats - View statistics\n"
            "/pending - View pending replies\n"
            "/help - Show this help\n\n"
            "**To reply to a duplicate:**\n"
            "Simply reply to any duplicate notification message with your text."
        )
        
        await update.message.reply_text(help_text, parse_mode="Markdown")
    
    def setup_handlers(self):
        """Setup bot handlers"""
        self.application.add_handler(CommandHandler("start", self.start_command))
        self.application.add_handler(CommandHandler("stats", self.stats_command))
        self.application.add_handler(CommandHandler("pending", self.pending_command))
        self.application.add_handler(CommandHandler("help", self.help_command))
        self.application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, 
                                                   self.handle_user_reply))
    
    def run(self):
        """Run the bot"""
        if not config.BOT_TOKEN:
            logger.error("BOT_TOKEN not configured!")
            return
        
        self.application = Application.builder().token(config.BOT_TOKEN).build()
        self.setup_handlers()
        
        logger.info("Starting Notifier Bot...")
        self.application.run_polling(drop_pending_updates=True)

def main():
    bot = NotifierBot()
    bot.run()

if __name__ == "__main__":
    main()
