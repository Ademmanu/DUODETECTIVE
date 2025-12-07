# database.py
import sqlite3
import threading
from datetime import datetime
from typing import List, Dict, Optional, Any
import os
import logging
import hashlib

logger = logging.getLogger("duplicate-database")

_thread_local = threading.local()

class DuplicateMonitorDB:
    def __init__(self, db_path: str = "duplicate_monitor.db"):
        self.db_path = db_path
        self.init_db()
    
    def _create_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        # Performance optimizations
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        return conn
    
    def get_connection(self) -> sqlite3.Connection:
        conn = getattr(_thread_local, "conn", None)
        if conn:
            try:
                conn.execute("SELECT 1")
                return conn
            except Exception:
                _thread_local.conn = None
        
        _thread_local.conn = self._create_connection()
        return _thread_local.conn
    
    def close_connection(self):
        conn = getattr(_thread_local, "conn", None)
        if conn:
            try:
                conn.close()
            except Exception:
                pass
            _thread_local.conn = None
    
    def init_db(self):
        conn = self.get_connection()
        cur = conn.cursor()
        
        # Users table (for monitoring sessions)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS monitoring_users (
                user_id INTEGER PRIMARY KEY,
                phone TEXT,
                name TEXT,
                session_data TEXT,
                is_active INTEGER DEFAULT 1,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            )
        """)
        
        # Monitored chats table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS monitored_chats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                chat_id INTEGER,
                chat_name TEXT,
                chat_type TEXT,
                added_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (user_id) REFERENCES monitoring_users (user_id),
                UNIQUE(user_id, chat_id)
            )
        """)
        
        # Messages table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER,
                message_id INTEGER,
                sender_id INTEGER,
                sender_name TEXT,
                message_text TEXT,
                message_hash TEXT,
                is_forwarded INTEGER DEFAULT 0,
                timestamp TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                UNIQUE(chat_id, message_id)
            )
        """)
        
        # Create index for faster duplicate detection
        cur.execute("CREATE INDEX IF NOT EXISTS idx_message_hash ON messages(chat_id, message_hash)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_message_timestamp ON messages(chat_id, timestamp)")
        
        # Duplicates table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS duplicates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER,
                original_message_id INTEGER,
                duplicate_message_id INTEGER,
                message_text TEXT,
                message_hash TEXT,
                status TEXT DEFAULT 'detected',  -- 'detected', 'notified', 'replied'
                bot_notification_id TEXT,
                user_reply TEXT,
                reply_sent INTEGER DEFAULT 0,
                detected_at TEXT DEFAULT (datetime('now')),
                replied_at TEXT,
                FOREIGN KEY (original_message_id) REFERENCES messages (id),
                FOREIGN KEY (duplicate_message_id) REFERENCES messages (id)
            )
        """)
        
        # Bot conversations table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bot_conversations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                duplicate_id INTEGER,
                bot_message_id INTEGER,
                chat_message_id TEXT,  -- Format: "chat_id:message_id"
                status TEXT DEFAULT 'waiting',  -- 'waiting', 'replied', 'processed'
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (duplicate_id) REFERENCES duplicates (id)
            )
        """)
        
        conn.commit()
    
    # User management
    def save_user(self, user_id: int, phone: str = None, name: str = None, 
                  session_data: str = None, is_active: bool = True):
        conn = self.get_connection()
        cur = conn.cursor()
        
        existing = self.get_user(user_id)
        if existing:
            cur.execute("""
                UPDATE monitoring_users 
                SET phone = COALESCE(?, phone),
                    name = COALESCE(?, name),
                    session_data = COALESCE(?, session_data),
                    is_active = ?,
                    updated_at = ?
                WHERE user_id = ?
            """, (phone, name, session_data, 1 if is_active else 0, 
                  datetime.now().isoformat(), user_id))
        else:
            cur.execute("""
                INSERT INTO monitoring_users (user_id, phone, name, session_data, is_active)
                VALUES (?, ?, ?, ?, ?)
            """, (user_id, phone, name, session_data, 1 if is_active else 0))
        
        conn.commit()
    
    def get_user(self, user_id: int) -> Optional[Dict]:
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute("SELECT * FROM monitoring_users WHERE user_id = ?", (user_id,))
        row = cur.fetchone()
        return dict(row) if row else None
    
    # Chat management
    def add_monitored_chat(self, user_id: int, chat_id: int, chat_name: str, chat_type: str):
        conn = self.get_connection()
        cur = conn.cursor()
        try:
            cur.execute("""
                INSERT INTO monitored_chats (user_id, chat_id, chat_name, chat_type)
                VALUES (?, ?, ?, ?)
            """, (user_id, chat_id, chat_name, chat_type))
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False
    
    def remove_monitored_chat(self, user_id: int, chat_id: int):
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute("DELETE FROM monitored_chats WHERE user_id = ? AND chat_id = ?", 
                   (user_id, chat_id))
        deleted = cur.rowcount > 0
        conn.commit()
        return deleted
    
    def get_user_monitored_chats(self, user_id: int) -> List[Dict]:
        conn = self.get_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, chat_id, chat_name, chat_type, added_at 
            FROM monitored_chats 
            WHERE user_id = ? 
            ORDER BY added_at DESC
        """, (user_id,))
        
        return [dict(row) for row in cur.fetchall()]
    
    # Message storage and duplicate detection
    def generate_message_hash(self, text: str) -> str:
        """Generate a hash for message text for duplicate detection"""
        # Normalize text: lowercase, remove extra whitespace
        normalized = ' '.join(text.strip().lower().split())
        return hashlib.md5(normalized.encode()).hexdigest()
    
    def save_message(self, chat_id: int, message_id: int, sender_id: int, 
                     sender_name: str, message_text: str, timestamp: datetime,
                     is_forwarded: bool = False) -> int:
        """Save a message and return message_id in database"""
        conn = self.get_connection()
        cur = conn.cursor()
        
        message_hash = self.generate_message_hash(message_text)
        
        cur.execute("""
            INSERT INTO messages (chat_id, message_id, sender_id, sender_name, 
                                 message_text, message_hash, is_forwarded, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(chat_id, message_id) DO UPDATE SET
                message_text = excluded.message_text,
                message_hash = excluded.message_hash,
                updated_at = CURRENT_TIMESTAMP
            RETURNING id
        """, (chat_id, message_id, sender_id, sender_name, 
              message_text, message_hash, 1 if is_forwarded else 0,
              timestamp.isoformat()))
        
        db_message_id = cur.fetchone()[0]
        conn.commit()
        return db_message_id
    
    def check_duplicate(self, chat_id: int, message_text: str, 
                        time_window_minutes: int = 60) -> Optional[Dict]:
        """Check if message is a duplicate within time window"""
        conn = self.get_connection()
        cur = conn.cursor()
        
        message_hash = self.generate_message_hash(message_text)
        
        # Look for same hash in same chat within time window
        cur.execute("""
            SELECT m.id, m.message_id, m.sender_name, m.message_text, m.timestamp
            FROM messages m
            WHERE m.chat_id = ? 
              AND m.message_hash = ?
              AND datetime(m.timestamp) >= datetime('now', ?)
            ORDER BY m.timestamp DESC
            LIMIT 1
        """, (chat_id, message_hash, f'-{time_window_minutes} minutes'))
        
        row = cur.fetchone()
        if row:
            return {
                'db_id': row[0],
                'message_id': row[1],
                'sender_name': row[2],
                'message_text': row[3],
                'timestamp': row[4]
            }
        return None
    
    def record_duplicate(self, chat_id: int, original_db_id: int, 
                         duplicate_db_id: int, message_text: str) -> int:
        """Record a duplicate detection"""
        conn = self.get_connection()
        cur = conn.cursor()
        
        message_hash = self.generate_message_hash(message_text)
        
        cur.execute("""
            INSERT INTO duplicates (chat_id, original_message_id, duplicate_message_id,
                                   message_text, message_hash, detected_at)
            VALUES (?, ?, ?, ?, ?, ?)
            RETURNING id
        """, (chat_id, original_db_id, duplicate_db_id, 
              message_text, message_hash, datetime.now().isoformat()))
        
        duplicate_id = cur.fetchone()[0]
        conn.commit()
        return duplicate_id
    
    # Bot notification handling
    def create_bot_conversation(self, user_id: int, duplicate_id: int, 
                               bot_message_id: int, chat_message_id: str):
        """Create a conversation for bot notification"""
        conn = self.get_connection()
        cur = conn.cursor()
        
        cur.execute("""
            INSERT INTO bot_conversations (user_id, duplicate_id, bot_message_id, 
                                          chat_message_id, status)
            VALUES (?, ?, ?, ?, 'waiting')
            RETURNING id
        """, (user_id, duplicate_id, bot_message_id, chat_message_id))
        
        conv_id = cur.fetchone()[0]
        
        # Update duplicate status
        cur.execute("""
            UPDATE duplicates 
            SET status = 'notified', bot_notification_id = ?
            WHERE id = ?
        """, (str(bot_message_id), duplicate_id))
        
        conn.commit()
        return conv_id
    
    def get_conversation_by_bot_message(self, bot_message_id: int) -> Optional[Dict]:
        """Get conversation by bot message ID"""
        conn = self.get_connection()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT c.*, d.chat_id, d.message_text, d.original_message_id
            FROM bot_conversations c
            JOIN duplicates d ON c.duplicate_id = d.id
            WHERE c.bot_message_id = ?
        """, (bot_message_id,))
        
        row = cur.fetchone()
        return dict(row) if row else None
    
    def save_user_reply(self, conversation_id: int, reply_text: str):
        """Save user's reply to duplicate notification"""
        conn = self.get_connection()
        cur = conn.cursor()
        
        cur.execute("""
            UPDATE bot_conversations 
            SET status = 'replied', 
                updated_at = ?
            WHERE id = ?
        """, (datetime.now().isoformat(), conversation_id))
        
        # Update duplicate record
        cur.execute("""
            UPDATE duplicates 
            SET status = 'replied',
                user_reply = ?,
                replied_at = ?
            WHERE id = (SELECT duplicate_id FROM bot_conversations WHERE id = ?)
        """, (reply_text, datetime.now().isoformat(), conversation_id))
        
        conn.commit()
    
    def mark_reply_sent(self, duplicate_id: int):
        """Mark that reply has been sent to chat"""
        conn = self.get_connection()
        cur = conn.cursor()
        
        cur.execute("""
            UPDATE duplicates 
            SET reply_sent = 1,
                status = 'completed'
            WHERE id = ?
        """, (duplicate_id,))
        
        conn.commit()
    
    # Statistics and monitoring
    def get_stats(self, user_id: int = None) -> Dict:
        """Get statistics for monitoring"""
        conn = self.get_connection()
        cur = conn.cursor()
        
        stats = {}
        
        # Total messages
        if user_id:
            # Get chats for user first
            cur.execute("SELECT chat_id FROM monitored_chats WHERE user_id = ?", (user_id,))
            user_chats = [row[0] for row in cur.fetchall()]
            if user_chats:
                placeholders = ','.join(['?'] * len(user_chats))
                cur.execute(f"SELECT COUNT(*) FROM messages WHERE chat_id IN ({placeholders})", 
                          user_chats)
            else:
                stats['total_messages'] = 0
                stats['total_duplicates'] = 0
                stats['pending_replies'] = 0
                return stats
        else:
            cur.execute("SELECT COUNT(*) FROM messages")
        
        stats['total_messages'] = cur.fetchone()[0]
        
        # Total duplicates
        if user_id:
            cur.execute("""
                SELECT COUNT(*) FROM duplicates d
                JOIN monitored_chats mc ON d.chat_id = mc.chat_id
                WHERE mc.user_id = ?
            """, (user_id,))
        else:
            cur.execute("SELECT COUNT(*) FROM duplicates")
        
        stats['total_duplicates'] = cur.fetchone()[0]
        
        # Pending replies
        if user_id:
            cur.execute("""
                SELECT COUNT(*) FROM duplicates d
                JOIN monitored_chats mc ON d.chat_id = mc.chat_id
                WHERE mc.user_id = ? AND d.status = 'replied' AND d.reply_sent = 0
            """, (user_id,))
        else:
            cur.execute("SELECT COUNT(*) FROM duplicates WHERE status = 'replied' AND reply_sent = 0")
        
        stats['pending_replies'] = cur.fetchone()[0]
        
        return stats
    
    def __del__(self):
        self.close_connection()
