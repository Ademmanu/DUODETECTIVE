# monitor.py
from telethon import TelegramClient, events
import asyncio
import aiohttp
from config import API_ID, API_HASH, BOT_TOKEN, MONITORED_CHATS, YOUR_USER_ID
from database import db
import json

class MessageMonitor:
    def __init__(self):
        self.client = TelegramClient('session', API_ID, API_HASH)
        self.bot_token = BOT_TOKEN
        self.your_user_id = YOUR_USER_ID
        
    async def start(self):
        await self.client.start()
        print("✅ Monitoring started. Listening for messages...")
        
        @self.client.on(events.NewMessage(chats=MONITORED_CHATS))
        async def handler(event):
            await self.handle_message(event)
        
        # Also listen for replies from bot
        @self.client.on(events.NewMessage(from_users=YOUR_USER_ID))
        async def handle_user_reply(event):
            if event.message.is_reply:
                await self.process_user_reply(event)
        
        await self.client.run_until_disconnected()
    
    async def handle_message(self, event):
        """Process incoming message"""
        chat_id = event.chat_id
        message_id = event.message.id
        sender_id = event.sender_id
        message_text = event.message.text or event.message.caption or ""
        
        # Save message to database
        content_hash = db.save_message(chat_id, message_id, sender_id, message_text)
        
        # Check for duplicate
        if db.is_duplicate(chat_id, content_hash):
            print(f"🚨 Duplicate detected in chat {chat_id}")
            await self.send_duplicate_alert(chat_id, message_id, message_text)
    
    async def send_duplicate_alert(self, chat_id, message_id, message_text):
        """Send alert to bot"""
        # Save alert to database
        alert_id = db.save_alert(chat_id, message_id, message_text)
        
        # Get chat info
        chat = await self.client.get_entity(chat_id)
        chat_title = getattr(chat, 'title', 'Private Chat')
        
        # Send to bot (via Telegram message to yourself)
        alert_msg = f"""
🚨 **DUPLICATE DETECTED**
**Chat:** {chat_title}
**Message ID:** {message_id}
**Alert ID:** {alert_id}
──────────────
**Message:**
{message_text[:200]}...
──────────────
Reply to this message with:
`/reply {alert_id} Your response here`
        """
        
        try:
            await self.client.send_message(self.your_user_id, alert_msg)
            print(f"✅ Alert sent for alert_id: {alert_id}")
        except Exception as e:
            print(f"❌ Failed to send alert: {e}")
    
    async def process_user_reply(self, event):
        """Process user's reply to alert"""
        reply_text = event.message.text
        original_msg = await event.message.get_reply_message()
        
        # Check if it's a reply to our alert message
        if original_msg and "DUPLICATE DETECTED" in original_msg.text:
            # Extract alert ID from original message
            lines = original_msg.text.split('\n')
            alert_id = None
            for line in lines:
                if "Alert ID:" in line:
                    alert_id = int(line.split(":")[1].strip())
                    break
            
            if alert_id and reply_text.startswith('/reply'):
                # Format: /reply <alert_id> <message>
                parts = reply_text.split(' ', 2)
                if len(parts) >= 3:
                    cmd, alert_id_str, response_text = parts
                    try:
                        alert_id = int(alert_id_str)
                        
                        # Get alert details from database
                        alerts = db.get_pending_alerts()
                        for alert in alerts:
                            if alert[0] == alert_id:
                                chat_id, message_id, _ = alert[1:4]
                                
                                # Send reply in original chat
                                await self.client.send_message(
                                    chat_id,
                                    response_text,
                                    reply_to=message_id
                                )
                                
                                # Mark as replied
                                db.mark_replied(alert_id, response_text)
                                print(f"✅ Reply sent to chat {chat_id}")
                                await event.reply(f"✅ Reply sent successfully!")
                                return
                        
                        await event.reply("❌ Alert not found or already replied")
                    except ValueError:
                        await event.reply("❌ Invalid alert ID format")
                else:
                    await event.reply("❌ Usage: /reply <alert_id> <message>")

async def main():
    monitor = MessageMonitor()
    await monitor.start()

if __name__ == "__main__":
    asyncio.run(main())
