#!/usr/bin/env python3
import os
import asyncio
import logging
import hashlib
from typing import Dict, List, Optional, Set, Tuple
from datetime import datetime, timedelta
import json

from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.types import User, Channel, Chat
from telethon.errors import SessionPasswordNeededError, FloodWaitError

from database import Database

# Optimized logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("monitor")

# Environment variables
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
NOTIFIER_BOT_TOKEN = os.getenv("NOTIFIER_BOT_TOKEN", "")  # Separate bot for notifications

# Performance tuning
MAX_CONCURRENT_USERS = int(os.getenv("MAX_CONCURRENT_USERS", "50"))
MESSAGE_BATCH_SIZE = int(os.getenv("MESSAGE_BATCH_SIZE", "5"))

# Initialize database
db = Database("duplicate_monitor.db")

# User session management
user_clients: Dict[int, TelegramClient] = {}
login_states: Dict[int, Dict] = {}
logout_states: Dict[int, Dict] = {}
task_creation_states: Dict[int, Dict] = {}

# Task caches
tasks_cache: Dict[int, List[Dict]] = {}
handler_registered: Dict[int, Callable] = {}

# Notifier bot (will be initialized later)
notifier_bot = None


# ========== Message Processing ==========
async def process_message(user_id: int, client: TelegramClient, event):
    """Process incoming message for duplicate detection"""
    try:
        message = event.message
        if not message or not message.text:
            return
        
        chat_id = event.chat_id
        message_id = message.id
        message_text = message.text.strip()
        sender_id = message.sender_id
        sender_username = getattr(message.sender, 'username', None)
        
        # Get user's active tasks that monitor this chat
        user_tasks = tasks_cache.get(user_id, [])
        
        for task in user_tasks:
            if not task.get('is_active', True):
                continue
            
            monitored_chats = task.get('monitored_chat_ids', [])
            if chat_id not in monitored_chats:
                continue
            
            # Check for duplicate
            is_duplicate, duplicate_message_id = await db.add_message(
                task_id=task['id'],
                chat_id=chat_id,
                message_id=message_id,
                message_text=message_text,
                sender_id=sender_id,
                sender_username=sender_username
            )
            
            if is_duplicate:
                logger.info(f"Duplicate detected in task {task['label']}, chat {chat_id}")
                
                # Create pending notification
                notification_id = await db.add_pending_notification(
                    task_id=task['id'],
                    chat_id=chat_id,
                    message_id=message_id,
                    duplicate_message_id=duplicate_message_id,
                    message_text=message_text,
                    sender_id=sender_id,
                    sender_username=sender_username
                )
                
                # Send notification to notifier bot
                await send_notification_to_bot(
                    user_id=user_id,
                    task_id=task['id'],
                    task_label=task['label'],
                    notification_id=notification_id,
                    chat_id=chat_id,
                    message_id=message_id,
                    message_text=message_text,
                    sender_id=sender_id,
                    sender_username=sender_username
                )
                
    except Exception as e:
        logger.exception(f"Error processing message: {e}")


# ========== Notification Sending ==========
async def send_notification_to_bot(user_id: int, task_id: int, task_label: str, 
                                  notification_id: int, chat_id: int, message_id: int,
                                  message_text: str, sender_id: int, sender_username: Optional[str]):
    """Send notification to the notifier bot"""
    try:
        # Format the notification
        notification_text = f"""
🚨 **DUPLICATE DETECTED!**

**Task:** {task_label}
**Chat ID:** `{chat_id}`
**Message ID:** `{message_id}`
**Sender:** {'@' + sender_username if sender_username else f'ID: {sender_id}'}
**Time:** {datetime.now().strftime('%H:%M:%S')}

**Message Content:** {message_text[:500]}{'...' if len(message_text) > 500 else ''}

**Notification ID:** `{notification_id}`

Please reply with your response to this duplicate message.
        """
        
        # In a real implementation, we would send this to the notifier bot
        # For now, we'll log it
        logger.info(f"Notification for user {user_id}: {notification_text}")
        
        # Mark as notified
        await db.update_notification_status(notification_id, 'notified')
        
    except Exception as e:
        logger.exception(f"Error sending notification: {e}")


# ========== Message Handler ==========
def ensure_handler_registered(user_id: int, client: TelegramClient):
    """Register message handler for monitoring"""
    if handler_registered.get(user_id):
        return
    
    @client.on(events.NewMessage())
    async def message_handler(event):
        await process_message(user_id, client, event)
    
    handler_registered[user_id] = message_handler
    logger.info(f"Registered message handler for user {user_id}")


# ========== Task Management ==========
async def load_user_tasks(user_id: int):
    """Load user's tasks into cache"""
    tasks = await db.get_user_tasks(user_id)
    tasks_cache[user_id] = tasks
    return tasks


async def start_monitoring_for_user(user_id: int):
    """Start monitoring for a user"""
    if user_id not in user_clients:
        return
    
    client = user_clients[user_id]
    tasks_cache.setdefault(user_id, [])
    
    # Load tasks
    await load_user_tasks(user_id)
    
    # Register handler
    ensure_handler_registered(user_id, client)
    
    logger.info(f"Started monitoring for user {user_id}")


# ========== Session Management ==========
async def restore_user_sessions():
    """Restore logged-in user sessions"""
    logger.info("Restoring user sessions...")
    
    users = await db.get_all_logged_in_users()
    
    for user in users:
        user_id = user['user_id']
        session_data = user['session_data']
        
        if not session_data:
            continue
        
        try:
            client = TelegramClient(StringSession(session_data), API_ID, API_HASH)
            await client.connect()
            
            if await client.is_user_authorized():
                user_clients[user_id] = client
                await start_monitoring_for_user(user_id)
                logger.info(f"Restored session for user {user_id}")
            else:
                # Session expired
                await db.save_user(user_id, is_logged_in=False)
                logger.warning(f"Session expired for user {user_id}")
                
        except Exception as e:
            logger.exception(f"Failed to restore session for user {user_id}: {e}")
            await db.save_user(user_id, is_logged_in=False)


# ========== Login Process ==========
async def login_user(user_id: int, phone: str, code: str = None, password: str = None) -> Tuple[bool, str]:
    """Login a user with phone number and verification"""
    try:
        client = TelegramClient(StringSession(), API_ID, API_HASH)
        await client.connect()
        
        if not code and not password:
            # Send code request
            await client.send_code_request(phone)
            login_states[user_id] = {
                'client': client,
                'phone': phone,
                'step': 'waiting_code'
            }
            return True, "Verification code sent"
        
        elif code and 'waiting_code' in login_states.get(user_id, {}):
            # Verify code
            state = login_states[user_id]
            try:
                await client.sign_in(state['phone'], code)
            except SessionPasswordNeededError:
                state['step'] = 'waiting_password'
                return True, "2FA password required"
            
            # Success
            me = await client.get_me()
            session_string = client.session.save()
            
            await db.save_user(
                user_id=user_id,
                phone=state['phone'],
                name=me.first_name or "User",
                session_data=session_string,
                is_logged_in=True
            )
            
            user_clients[user_id] = client
            await start_monitoring_for_user(user_id)
            
            del login_states[user_id]
            return True, "Login successful"
        
        elif password and 'waiting_password' in login_states.get(user_id, {}):
            # Verify 2FA password
            state = login_states[user_id]
            try:
                await client.sign_in(password=password)
            except Exception as e:
                return False, f"2FA failed: {str(e)}"
            
            # Success
            me = await client.get_me()
            session_string = client.session.save()
            
            await db.save_user(
                user_id=user_id,
                phone=state['phone'],
                name=me.first_name or "User",
                session_data=session_string,
                is_logged_in=True
            )
            
            user_clients[user_id] = client
            await start_monitoring_for_user(user_id)
            
            del login_states[user_id]
            return True, "Login successful with 2FA"
        
        else:
            return False, "Invalid login state"
            
    except FloodWaitError as fwe:
        wait_time = getattr(fwe, 'seconds', 60)
        return False, f"Flood wait: Please try again in {wait_time} seconds"
    except Exception as e:
        logger.exception(f"Login error for user {user_id}")
        return False, f"Login failed: {str(e)}"


async def logout_user(user_id: int) -> bool:
    """Logout a user"""
    try:
        if user_id in user_clients:
            client = user_clients[user_id]
            await client.disconnect()
            user_clients.pop(user_id, None)
        
        handler_registered.pop(user_id, None)
        tasks_cache.pop(user_id, None)
        
        await db.save_user(user_id, is_logged_in=False)
        return True
        
    except Exception as e:
        logger.exception(f"Logout error for user {user_id}")
        return False


# ========== Task Operations ==========
async def create_monitor_task(user_id: int, label: str, monitored_chat_ids: List[int],
                            duplicate_window_hours: int = 24, detection_method: str = 'hash') -> Tuple[bool, str]:
    """Create a new monitor task"""
    try:
        success = await db.add_monitor_task(
            user_id=user_id,
            label=label,
            monitored_chat_ids=monitored_chat_ids,
            duplicate_window_hours=duplicate_window_hours,
            detection_method=detection_method
        )
        
        if success:
            await load_user_tasks(user_id)
            return True, f"Task '{label}' created successfully"
        else:
            return False, f"Task '{label}' already exists"
            
    except Exception as e:
        logger.exception(f"Error creating task for user {user_id}")
        return False, f"Error creating task: {str(e)}"


async def delete_monitor_task(user_id: int, label: str) -> Tuple[bool, str]:
    """Delete a monitor task"""
    try:
        success = await db.remove_monitor_task(user_id, label)
        
        if success:
            await load_user_tasks(user_id)
            return True, f"Task '{label}' deleted successfully"
        else:
            return False, f"Task '{label}' not found"
            
    except Exception as e:
        logger.exception(f"Error deleting task for user {user_id}")
        return False, f"Error deleting task: {str(e)}"


async def toggle_task_status(user_id: int, label: str, is_active: bool) -> Tuple[bool, str]:
    """Toggle task active status"""
    try:
        tasks = tasks_cache.get(user_id, [])
        task = next((t for t in tasks if t['label'] == label), None)
        
        if not task:
            return False, f"Task '{label}' not found"
        
        success = await db.update_monitor_task(task['id'], is_active=is_active)
        
        if success:
            await load_user_tasks(user_id)
            status = "activated" if is_active else "deactivated"
            return True, f"Task '{label}' {status} successfully"
        else:
            return False, f"Failed to update task '{label}'"
            
    except Exception as e:
        logger.exception(f"Error toggling task for user {user_id}")
        return False, f"Error updating task: {str(e)}"


# ========== Statistics ==========
async def get_user_stats(user_id: int) -> Dict:
    """Get user statistics"""
    try:
        tasks = await db.get_user_tasks(user_id)
        stats = {
            'total_tasks': len(tasks),
            'active_tasks': sum(1 for t in tasks if t.get('is_active', True)),
            'total_duplicates': 0,
            'pending_notifications': 0
        }
        
        for task in tasks:
            task_stats = await db.get_task_stats(task['id'])
            stats['total_duplicates'] += task_stats.get('duplicates_found', 0)
            stats['pending_notifications'] += task_stats.get('pending_notifications', 0)
        
        return stats
        
    except Exception as e:
        logger.exception(f"Error getting stats for user {user_id}")
        return {}


# ========== Chat ID Discovery ==========
async def get_user_chats(user_id: int, category: str = "all") -> List[Dict]:
    """Get user's chats categorized"""
    if user_id not in user_clients:
        return []
    
    client = user_clients[user_id]
    chats = []
    
    try:
        async for dialog in client.iter_dialogs(limit=100):
            entity = dialog.entity
            
            chat_info = {
                'id': dialog.id,
                'name': dialog.name,
                'type': None,
                'username': getattr(entity, 'username', None)
            }
            
            # Categorize
            if isinstance(entity, User):
                if entity.bot:
                    chat_info['type'] = 'bot'
                else:
                    chat_info['type'] = 'private'
            elif isinstance(entity, Channel):
                if getattr(entity, 'broadcast', False):
                    chat_info['type'] = 'channel'
                else:
                    chat_info['type'] = 'group'
            elif isinstance(entity, Chat):
                chat_info['type'] = 'group'
            
            # Filter by category
            if category == 'all' or chat_info['type'] == category:
                chats.append(chat_info)
                
    except Exception as e:
        logger.exception(f"Error getting chats for user {user_id}")
    
    return chats


# ========== Reply Processing ==========
async def send_reply(user_id: int, chat_id: int, message_id: int, reply_text: str) -> Tuple[bool, str]:
    """Send reply to a duplicate message"""
    if user_id not in user_clients:
        return False, "User not logged in"
    
    try:
        client = user_clients[user_id]
        
        # Get the chat entity
        entity = await client.get_input_entity(chat_id)
        
        # Send reply
        await client.send_message(
            entity=entity,
            message=reply_text,
            reply_to=message_id
        )
        
        logger.info(f"Reply sent by user {user_id} to chat {chat_id}, message {message_id}")
        return True, "Reply sent successfully"
        
    except FloodWaitError as fwe:
        wait_time = getattr(fwe, 'seconds', 60)
        return False, f"Flood wait: Please try again in {wait_time} seconds"
    except Exception as e:
        logger.exception(f"Error sending reply for user {user_id}")
        return False, f"Failed to send reply: {str(e)}"


# ========== Main Loop ==========
async def main():
    """Main monitoring loop"""
    logger.info("Starting Duplicate Message Monitor...")
    
    # Restore sessions
    await restore_user_sessions()
    
    logger.info(f"Monitoring {len(user_clients)} users")
    
    # Keep the script running
    try:
        while True:
            await asyncio.sleep(3600)  # Sleep for 1 hour
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        # Cleanup
        for user_id, client in list(user_clients.items()):
            try:
                await client.disconnect()
            except Exception:
                pass


if __name__ == "__main__":
    asyncio.run(main())
