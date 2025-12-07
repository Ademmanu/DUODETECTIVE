# monitor.py - Now returns async function
from telethon import TelegramClient, events
import asyncio
from config import API_ID, API_HASH, BOT_TOKEN, MONITORED_CHATS, YOUR_USER_ID
from database import db

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
        
        # Listen for replies from user
        @self.client.on(events.NewMessage(from_users=YOUR_USER_ID))
        async def handle_user_reply(event):
            if event.message.is_reply:
                await self.process_user_reply(event)
        
        print(f"👁️  Monitoring {len(MONITORED_CHATS)} chat(s)")
        print("Press Ctrl+C to stop")
        
        # Keep running
        await self.client.run_until_disconnected()
    
    async def handle_message(self, event):
        """Process incoming message"""
        try:
            chat_id = event.chat_id
            message_id = event.message.id
            sender_id = event.sender_id
            message_text = event.message.text or event.message.caption or ""
            
            # Skip very short messages
            if len(message_text) < 3:
                return
            
            # Save message to database
            content_hash = db.save_message(chat_id, message_id, sender_id, message_text)
            
            # Check for duplicate (in last 100 messages)
            if db.is_duplicate(chat_id, content_hash):
                print(f"🚨 Duplicate detected in chat {chat_id}")
                await self.send_duplicate_alert(chat_id, message_id, message_text, sender_id)
        except Exception as e:
            print(f"❌ Error handling message: {e}")
    
    async def send_duplicate_alert(self, chat_id, message_id, message_text, sender_id):
        """Send alert to user"""
        alert_id = db.save_alert(chat_id, message_id, message_text)
        
        try:
            # Get chat info
            chat = await self.client.get_entity(chat_id)
            chat_title = getattr(chat, 'title', f'Chat {chat_id}')
            
            # Get sender info
            sender = await self.client.get_entity(sender_id)
            sender_name = getattr(sender, 'username', getattr(sender, 'first_name', str(sender_id)))
            
            alert_msg = f"""
🚨 **DUPLICATE DETECTED**
**Chat:** {chat_title}
**From:** {sender_name}
**Message ID:** {message_id}
**Alert ID:** `{alert_id}`
──────────────
**Message:**
{message_text[:300]}
──────────────
Reply to this message with:
`/reply {alert_id} Your response here`
            """
            
            await self.client.send_message(self.your_user_id, alert_msg)
            print(f"✅ Alert {alert_id} sent")
            
        except Exception as e:
            print(f"❌ Failed to send alert: {e}")
            # Try to send simple alert
            try:
                simple_alert = f"🚨 Duplicate in chat {chat_id}. Alert ID: {alert_id}"
                await self.client.send_message(self.your_user_id, simple_alert)
            except:
                pass
    
    async def process_user_reply(self, event):
        """Process user's reply to alert"""
        try:
            reply_text = event.message.text.strip()
            original_msg = await event.message.get_reply_message()
            
            if not original_msg or "DUPLICATE DETECTED" not in original_msg.text:
                return
            
            if reply_text.startswith('/reply'):
                parts = reply_text.split(' ', 2)
                if len(parts) >= 3:
                    _, alert_id_str, response_text = parts
                    
                    try:
                        alert_id = int(alert_id_str)
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
                                await event.reply(f"✅ Reply sent!")
                                return
                        
                        await event.reply("❌ Alert not found")
                    except ValueError:
                        await event.reply("❌ Invalid alert ID")
                else:
                    await event.reply("❌ Usage: /reply <alert_id> <message>")
        except Exception as e:
            print(f"❌ Error processing reply: {e}")
            await event.reply(f"❌ Error: {str(e)}")

async def main():
    monitor = MessageMonitor()
    await monitor.start()
