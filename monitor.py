# monitor.py
#!/usr/bin/env python3
import asyncio
import logging
import sys
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.types import Message, User, Channel, Chat
import config
from database import DuplicateMonitorDB

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("duplicate-monitor")

db = DuplicateMonitorDB()

class DuplicateMonitor:
    def __init__(self):
        self.user_clients: Dict[int, TelegramClient] = {}
        self.active_monitors: Dict[int, Set[int]] = {}  # user_id -> set of chat_ids
        self.login_states: Dict[int, Dict] = {}
        
    async def login_user(self, user_id: int, phone: str, session_string: str = None) -> bool:
        """Login a user with Telethon"""
        try:
            if session_string:
                client = TelegramClient(StringSession(session_string), config.API_ID, config.API_HASH)
            else:
                client = TelegramClient(StringSession(), config.API_ID, config.API_HASH)
            
            await client.connect()
            
            if not await client.is_user_authorized():
                # Need to login with phone
                await client.send_code_request(phone)
                # In actual implementation, you'd wait for code input
                return False
            
            me = await client.get_me()
            session_string = client.session.save()
            
            # Save user session
            db.save_user(user_id, phone, me.first_name, session_string, True)
            self.user_clients[user_id] = client
            
            # Load monitored chats
            chats = db.get_user_monitored_chats(user_id)
            self.active_monitors[user_id] = {chat['chat_id'] for chat in chats}
            
            # Start monitoring
            await self.start_monitoring_user(user_id, client)
            
            logger.info(f"User {user_id} logged in and monitoring {len(chats)} chats")
            return True
            
        except Exception as e:
            logger.error(f"Login failed for user {user_id}: {e}")
            return False
    
    async def start_monitoring_user(self, user_id: int, client: TelegramClient):
        """Start monitoring chats for a user"""
        @client.on(events.NewMessage())
        async def message_handler(event):
            await self.handle_new_message(user_id, event)
        
        @client.on(events.MessageEdited())
        async def message_edit_handler(event):
            await self.handle_new_message(user_id, event)
    
    async def handle_new_message(self, user_id: int, event):
        """Process incoming messages for duplicate detection"""
        try:
            # Check if chat is being monitored
            chat_id = event.chat_id
            if user_id not in self.active_monitors or chat_id not in self.active_monitors[user_id]:
                return
            
            message = event.message
            if not message or not message.text:
                return
            
            # Get sender info
            sender_id = message.sender_id
            sender_name = "Unknown"
            try:
                sender = await message.get_sender()
                if isinstance(sender, User):
                    sender_name = f"{sender.first_name or ''} {sender.last_name or ''}".strip()
                    if not sender_name and sender.username:
                        sender_name = f"@{sender.username}"
                elif isinstance(sender, Channel):
                    sender_name = sender.title
            except:
                pass
            
            # Clean message text
            message_text = message.text.strip()
            if not message_text:
                return
            
            # Check for duplicate
            original = db.check_duplicate(chat_id, message_text, config.DUPLICATE_TIME_WINDOW // 60)
            
            if original:
                # Save duplicate message
                duplicate_db_id = db.save_message(
                    chat_id=chat_id,
                    message_id=message.id,
                    sender_id=sender_id,
                    sender_name=sender_name,
                    message_text=message_text,
                    timestamp=message.date,
                    is_forwarded=message.forward
                )
                
                # Record duplicate
                duplicate_id = db.record_duplicate(
                    chat_id=chat_id,
                    original_db_id=original['db_id'],
                    duplicate_db_id=duplicate_db_id,
                    message_text=message_text
                )
                
                # Get chat info
                chat = await event.get_chat()
                chat_name = getattr(chat, 'title', 'Unknown Chat')
                if not chat_name and hasattr(chat, 'username'):
                    chat_name = f"@{chat.username}"
                
                # Format notification
                notification = self.format_duplicate_notification(
                    chat_name=chat_name,
                    message_text=message_text,
                    original_sender=original['sender_name'],
                    duplicate_sender=sender_name,
                    timestamp=message.date,
                    duplicate_id=duplicate_id
                )
                
                # Send to notification bot (would be implemented with a queue or direct API call)
                # For now, log it
                logger.info(f"Duplicate detected! {notification[:100]}...")
                
                # In actual implementation, you'd send this to the notification bot
                # await self.send_to_notification_bot(user_id, notification, duplicate_id, chat_id, message.id)
                
            else:
                # Save as new message
                db.save_message(
                    chat_id=chat_id,
                    message_id=message.id,
                    sender_id=sender_id,
                    sender_name=sender_name,
                    message_text=message_text,
                    timestamp=message.date,
                    is_forwarded=message.forward
                )
                
        except Exception as e:
            logger.error(f"Error processing message for user {user_id}: {e}")
    
    def format_duplicate_notification(self, chat_name: str, message_text: str, 
                                     original_sender: str, duplicate_sender: str,
                                     timestamp: datetime, duplicate_id: int) -> str:
        """Format duplicate notification message"""
        # Truncate long messages
        if len(message_text) > 200:
            display_text = message_text[:197] + "..."
        else:
            display_text = message_text
        
        # Escape markdown characters
        display_text = self.escape_markdown(display_text)
        
        return (
            "🚨 **Duplicate Message Detected!**\n\n"
            f"**Chat:** {self.escape_markdown(chat_name)}\n"
            f"**Original Sender:** {self.escape_markdown(original_sender)}\n"
            f"**Duplicate Sender:** {self.escape_markdown(duplicate_sender)}\n"
            f"**Time:** {timestamp.strftime('%Y-%m-%d %H:%M:%S')}\n\n"
            "**Message:**\n"
            f"```\n{display_text}\n```\n\n"
            f"**ID:** `{duplicate_id}`\n\n"
            "Please reply to this message with your response.\n"
            "Your reply will be sent to the chat where the duplicate occurred."
        )
    
    def escape_markdown(self, text: str) -> str:
        """Escape markdown special characters"""
        escape_chars = r'_*[]()~`>#+-=|{}.!'
        for char in escape_chars:
            text = text.replace(char, f'\\{char}')
        return text
    
    async def send_reply_to_chat(self, user_id: int, duplicate_id: int, reply_text: str) -> bool:
        """Send user's reply to the original chat"""
        try:
            if user_id not in self.user_clients:
                logger.error(f"No active client for user {user_id}")
                return False
            
            client = self.user_clients[user_id]
            
            # Get duplicate info from database
            conn = db.get_connection()
            cur = conn.cursor()
            cur.execute("""
                SELECT d.chat_id, d.duplicate_message_id, d.message_text
                FROM duplicates d
                WHERE d.id = ? AND d.status = 'replied' AND d.reply_sent = 0
            """, (duplicate_id,))
            
            result = cur.fetchone()
            if not result:
                logger.error(f"No pending reply found for duplicate {duplicate_id}")
                return False
            
            chat_id, message_id, original_text = result
            
            # Send reply
            try:
                entity = await client.get_input_entity(int(chat_id))
                await client.send_message(
                    entity,
                    reply_text,
                    reply_to=message_id
                )
                
                # Mark as sent
                db.mark_reply_sent(duplicate_id)
                logger.info(f"Reply sent to chat {chat_id} for duplicate {duplicate_id}")
                return True
                
            except Exception as e:
                logger.error(f"Failed to send reply to chat {chat_id}: {e}")
                return False
                
        except Exception as e:
            logger.error(f"Error sending reply for duplicate {duplicate_id}: {e}")
            return False
    
    async def add_monitored_chat(self, user_id: int, chat_id: int) -> bool:
        """Add a chat to monitor"""
        try:
            if user_id not in self.user_clients:
                return False
            
            client = self.user_clients[user_id]
            
            # Get chat info
            try:
                chat = await client.get_entity(int(chat_id))
                chat_name = getattr(chat, 'title', 'Unknown')
                if not chat_name and hasattr(chat, 'username'):
                    chat_name = f"@{chat.username}"
                
                chat_type = self.get_chat_type(chat)
                
                # Save to database
                success = db.add_monitored_chat(user_id, chat_id, chat_name, chat_type)
                
                if success:
                    if user_id not in self.active_monitors:
                        self.active_monitors[user_id] = set()
                    self.active_monitors[user_id].add(chat_id)
                    logger.info(f"Added chat {chat_name} ({chat_id}) to monitor for user {user_id}")
                
                return success
                
            except Exception as e:
                logger.error(f"Failed to get chat info for {chat_id}: {e}")
                return False
                
        except Exception as e:
            logger.error(f"Error adding monitored chat for user {user_id}: {e}")
            return False
    
    def get_chat_type(self, chat) -> str:
        """Determine chat type"""
        if isinstance(chat, User):
            return 'private'
        elif isinstance(chat, Channel):
            if getattr(chat, 'broadcast', False):
                return 'channel'
            else:
                return 'supergroup'
        elif isinstance(chat, Chat):
            return 'group'
        return 'unknown'
    
    async def shutdown(self):
        """Clean shutdown"""
        for user_id, client in self.user_clients.items():
            try:
                await client.disconnect()
            except Exception as e:
                logger.error(f"Error disconnecting client for user {user_id}: {e}")
        
        db.close_connection()

# Main function
async def main():
    monitor = DuplicateMonitor()
    
    # Example: Login a user (in real app, this would be triggered via bot command)
    # user_id = 123456789
    # phone = "+1234567890"
    # session_string = None  # or existing session
    
    # success = await monitor.login_user(user_id, phone, session_string)
    # if success:
    #     print(f"User {user_id} logged in successfully")
    
    # Keep running
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        await monitor.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
