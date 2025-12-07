import sqlite3
import json
import threading
import hashlib
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any, Tuple
import os
import logging

logger = logging.getLogger("database")

"""
Database for Duplicate Message Monitor System
"""

_thread_local = threading.local()


class Database:
    def __init__(self, db_path: str = "duplicate_monitor.db"):
        self.db_path = db_path
        self.init_db()

    def _create_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        # Performance optimizations
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA temp_store=MEMORY;")
        conn.execute("PRAGMA cache_size=-2000;")
        return conn

    def get_connection(self) -> sqlite3.Connection:
        """Return a thread-local connection"""
        conn = getattr(_thread_local, "conn", None)
        if conn:
            try:
                conn.execute("SELECT 1")
                return conn
            except Exception:
                try:
                    conn.close()
                except Exception:
                    pass
                _thread_local.conn = None
        
        _thread_local.conn = self._create_connection()
        return _thread_local.conn

    def init_db(self):
        """Initialize database tables"""
        conn = self.get_connection()
        cur = conn.cursor()
        
        # Users table (for authentication)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                phone TEXT,
                name TEXT,
                session_data TEXT,
                is_logged_in INTEGER DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            )
        """)
        
        # Allowed users table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS allowed_users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                is_admin INTEGER DEFAULT 0,
                added_by INTEGER,
                created_at TEXT DEFAULT (datetime('now'))
            )
        """)
        
        # Monitor tasks table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS monitor_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                label TEXT,
                monitored_chat_ids TEXT,  -- JSON array of chat IDs to monitor
                duplicate_window_hours INTEGER DEFAULT 24,
                detection_method TEXT DEFAULT 'hash',  -- 'hash' or 'exact'
                is_active INTEGER DEFAULT 1,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (user_id) REFERENCES users (user_id),
                UNIQUE(user_id, label)
            )
        """)
        
        # Messages table for duplicate detection
        cur.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id INTEGER,
                chat_id INTEGER,
                message_id INTEGER,
                message_hash TEXT,
                message_text TEXT,
                sender_id INTEGER,
                sender_username TEXT,
                timestamp TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (task_id) REFERENCES monitor_tasks (id),
                UNIQUE(task_id, chat_id, message_hash)
            )
        """)
        
        # Create index for faster duplicate detection
        cur.execute("CREATE INDEX IF NOT EXISTS idx_message_hash ON messages(message_hash, task_id, chat_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON messages(timestamp)")
        
        # Pending notifications table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS pending_notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id INTEGER,
                chat_id INTEGER,
                message_id INTEGER,
                duplicate_message_id INTEGER,  -- The original message that was duplicated
                message_text TEXT,
                sender_id INTEGER,
                sender_username TEXT,
                status TEXT DEFAULT 'pending',  -- 'pending', 'notified', 'replied'
                notified_at TEXT,
                replied_at TEXT,
                reply_text TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (task_id) REFERENCES monitor_tasks (id)
            )
        """)
        
        conn.commit()

    # ========== User Management ==========
    def get_user(self, user_id: int) -> Optional[Dict]:
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        row = cur.fetchone()
        if not row:
            return None
        return dict(row)

    def save_user(self, user_id: int, phone: Optional[str] = None, name: Optional[str] = None,
                  session_data: Optional[str] = None, is_logged_in: bool = False):
        conn = self.get_connection()
        cur = conn.cursor()
        
        existing = self.get_user(user_id)
        if existing:
            updates = []
            params = []
            
            if phone is not None:
                updates.append("phone = ?")
                params.append(phone)
            if name is not None:
                updates.append("name = ?")
                params.append(name)
            if session_data is not None:
                updates.append("session_data = ?")
                params.append(session_data)
            
            updates.append("is_logged_in = ?")
            params.append(1 if is_logged_in else 0)
            updates.append("updated_at = ?")
            params.append(datetime.now().isoformat())
            
            params.append(user_id)
            query = f"UPDATE users SET {', '.join(updates)} WHERE user_id = ?"
            cur.execute(query, params)
        else:
            cur.execute("""
                INSERT INTO users (user_id, phone, name, session_data, is_logged_in)
                VALUES (?, ?, ?, ?, ?)
            """, (user_id, phone, name, session_data, 1 if is_logged_in else 0))
        
        conn.commit()

    # ========== Task Management ==========
    def add_monitor_task(self, user_id: int, label: str, monitored_chat_ids: List[int],
                        duplicate_window_hours: int = 24, detection_method: str = 'hash') -> bool:
        conn = self.get_connection()
        cur = conn.cursor()
        
        try:
            cur.execute("""
                INSERT INTO monitor_tasks (user_id, label, monitored_chat_ids, duplicate_window_hours, detection_method)
                VALUES (?, ?, ?, ?, ?)
            """, (user_id, label, json.dumps(monitored_chat_ids), duplicate_window_hours, detection_method))
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False

    def update_monitor_task(self, task_id: int, **kwargs) -> bool:
        conn = self.get_connection()
        cur = conn.cursor()
        
        updates = []
        params = []
        
        for key, value in kwargs.items():
            if key == 'monitored_chat_ids' and isinstance(value, list):
                updates.append(f"{key} = ?")
                params.append(json.dumps(value))
            elif key in ['label', 'duplicate_window_hours', 'detection_method', 'is_active']:
                updates.append(f"{key} = ?")
                params.append(value)
        
        if not updates:
            return False
        
        updates.append("updated_at = ?")
        params.append(datetime.now().isoformat())
        params.append(task_id)
        
        query = f"UPDATE monitor_tasks SET {', '.join(updates)} WHERE id = ?"
        cur.execute(query, params)
        conn.commit()
        return cur.rowcount > 0

    def remove_monitor_task(self, user_id: int, label: str) -> bool:
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute("DELETE FROM monitor_tasks WHERE user_id = ? AND label = ?", (user_id, label))
        deleted = cur.rowcount > 0
        conn.commit()
        return deleted

    def get_user_tasks(self, user_id: int) -> List[Dict]:
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT * FROM monitor_tasks 
            WHERE user_id = ? 
            ORDER BY created_at DESC
        """, (user_id,))
        
        tasks = []
        for row in cur.fetchall():
            task = dict(row)
            task['monitored_chat_ids'] = json.loads(task['monitored_chat_ids']) if task['monitored_chat_ids'] else []
            tasks.append(task)
        return tasks

    def get_all_active_tasks(self) -> List[Dict]:
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute("SELECT * FROM monitor_tasks WHERE is_active = 1")
        
        tasks = []
        for row in cur.fetchall():
            task = dict(row)
            task['monitored_chat_ids'] = json.loads(task['monitored_chat_ids']) if task['monitored_chat_ids'] else []
            tasks.append(task)
        return tasks

    def get_task_by_id(self, task_id: int) -> Optional[Dict]:
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute("SELECT * FROM monitor_tasks WHERE id = ?", (task_id,))
        row = cur.fetchone()
        if not row:
            return None
        task = dict(row)
        task['monitored_chat_ids'] = json.loads(task['monitored_chat_ids']) if task['monitored_chat_ids'] else []
        return task

    # ========== Message Management ==========
    def add_message(self, task_id: int, chat_id: int, message_id: int, message_text: str,
                   sender_id: int, sender_username: Optional[str] = None) -> Tuple[bool, Optional[int]]:
        """Add a message and check if it's a duplicate.
           Returns: (is_duplicate, duplicate_message_id)"""
        conn = self.get_connection()
        cur = conn.cursor()
        
        # Calculate message hash
        message_hash = hashlib.md5(message_text.encode()).hexdigest()
        
        # Check if this is a duplicate within the time window
        task = self.get_task_by_id(task_id)
        if not task:
            return False, None
        
        window_hours = task.get('duplicate_window_hours', 24)
        cutoff_time = (datetime.now() - timedelta(hours=window_hours)).isoformat()
        
        # Check for duplicate based on detection method
        if task.get('detection_method') == 'exact':
            cur.execute("""
                SELECT id FROM messages 
                WHERE task_id = ? AND chat_id = ? AND message_text = ? 
                AND timestamp > ?
                LIMIT 1
            """, (task_id, chat_id, message_text, cutoff_time))
        else:  # hash method
            cur.execute("""
                SELECT id FROM messages 
                WHERE task_id = ? AND chat_id = ? AND message_hash = ? 
                AND timestamp > ?
                LIMIT 1
            """, (task_id, chat_id, message_hash, cutoff_time))
        
        duplicate_row = cur.fetchone()
        
        # Always store the message for future duplicate detection
        try:
            cur.execute("""
                INSERT INTO messages (task_id, chat_id, message_id, message_hash, 
                                     message_text, sender_id, sender_username, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (task_id, chat_id, message_id, message_hash, message_text, 
                  sender_id, sender_username, datetime.now().isoformat()))
            conn.commit()
        except sqlite3.IntegrityError:
            # Message already exists (same hash for same task/chat)
            pass
        
        # Clean up old messages outside the time window
        self._cleanup_old_messages(task_id, window_hours)
        
        if duplicate_row:
            return True, duplicate_row['id']
        return False, None

    def _cleanup_old_messages(self, task_id: int, window_hours: int):
        """Remove messages older than the duplicate window"""
        conn = self.get_connection()
        cur = conn.cursor()
        cutoff_time = (datetime.now() - timedelta(hours=window_hours * 2)).isoformat()
        cur.execute("DELETE FROM messages WHERE task_id = ? AND timestamp < ?", 
                   (task_id, cutoff_time))
        conn.commit()

    # ========== Notification Management ==========
    def add_pending_notification(self, task_id: int, chat_id: int, message_id: int,
                                duplicate_message_id: int, message_text: str,
                                sender_id: int, sender_username: Optional[str] = None) -> int:
        """Add a pending notification and return its ID"""
        conn = self.get_connection()
        cur = conn.cursor()
        
        cur.execute("""
            INSERT INTO pending_notifications 
            (task_id, chat_id, message_id, duplicate_message_id, message_text, 
             sender_id, sender_username, notified_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (task_id, chat_id, message_id, duplicate_message_id, message_text,
              sender_id, sender_username, datetime.now().isoformat()))
        
        notification_id = cur.lastrowid
        conn.commit()
        return notification_id

    def update_notification_status(self, notification_id: int, status: str, reply_text: Optional[str] = None):
        conn = self.get_connection()
        cur = conn.cursor()
        
        if status == 'replied' and reply_text:
            cur.execute("""
                UPDATE pending_notifications 
                SET status = ?, reply_text = ?, replied_at = ?
                WHERE id = ?
            """, (status, reply_text, datetime.now().isoformat(), notification_id))
        else:
            cur.execute("""
                UPDATE pending_notifications 
                SET status = ?
                WHERE id = ?
            """, (status, notification_id))
        
        conn.commit()

    def get_pending_notification(self, notification_id: int) -> Optional[Dict]:
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute("SELECT * FROM pending_notifications WHERE id = ?", (notification_id,))
        row = cur.fetchone()
        return dict(row) if row else None

    def get_pending_notifications_by_task(self, task_id: int, status: str = 'pending') -> List[Dict]:
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT * FROM pending_notifications 
            WHERE task_id = ? AND status = ?
            ORDER BY created_at DESC
        """, (task_id, status))
        
        return [dict(row) for row in cur.fetchall()]

    # ========== User Authorization ==========
    def is_user_allowed(self, user_id: int) -> bool:
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute("SELECT user_id FROM allowed_users WHERE user_id = ?", (user_id,))
        return cur.fetchone() is not None

    def is_user_admin(self, user_id: int) -> bool:
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute("SELECT is_admin FROM allowed_users WHERE user_id = ?", (user_id,))
        row = cur.fetchone()
        return row and int(row['is_admin']) == 1

    def add_allowed_user(self, user_id: int, username: Optional[str] = None, 
                        is_admin: bool = False, added_by: Optional[int] = None) -> bool:
        conn = self.get_connection()
        cur = conn.cursor()
        
        try:
            cur.execute("""
                INSERT INTO allowed_users (user_id, username, is_admin, added_by)
                VALUES (?, ?, ?, ?)
            """, (user_id, username, 1 if is_admin else 0, added_by))
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False

    def remove_allowed_user(self, user_id: int) -> bool:
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute("DELETE FROM allowed_users WHERE user_id = ?", (user_id,))
        deleted = cur.rowcount > 0
        conn.commit()
        return deleted

    def get_all_allowed_users(self) -> List[Dict]:
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute("SELECT * FROM allowed_users ORDER BY created_at DESC")
        return [dict(row) for row in cur.fetchall()]

    # ========== Statistics ==========
    def get_task_stats(self, task_id: int) -> Dict:
        conn = self.get_connection()
        cur = conn.cursor()
        
        stats = {}
        
        # Count total messages
        cur.execute("SELECT COUNT(*) as count FROM messages WHERE task_id = ?", (task_id,))
        stats['total_messages'] = cur.fetchone()['count']
        
        # Count duplicates
        cur.execute("""
            SELECT COUNT(DISTINCT m2.id) as count 
            FROM messages m1
            JOIN messages m2 ON m1.message_hash = m2.message_hash 
                AND m1.chat_id = m2.chat_id 
                AND m1.id != m2.id
            WHERE m1.task_id = ?
        """, (task_id,))
        stats['duplicates_found'] = cur.fetchone()['count']
        
        # Count pending notifications
        cur.execute("""
            SELECT COUNT(*) as count 
            FROM pending_notifications 
            WHERE task_id = ? AND status = 'pending'
        """, (task_id,))
        stats['pending_notifications'] = cur.fetchone()['count']
        
        return stats

    def close_connection(self):
        """Close thread-local connection"""
        conn = getattr(_thread_local, "conn", None)
        if conn:
            try:
                conn.close()
            except Exception:
                pass
            _thread_local.conn = None

    def __del__(self):
        try:
            self.close_connection()
        except Exception:
            pass
