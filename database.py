import sqlite3
import json
import threading
from datetime import datetime
from typing import List, Dict, Optional, Any
import os
import logging

logger = logging.getLogger("database")

_conn_init_lock = threading.Lock()
_thread_local = threading.local()


class Database:
    def __init__(self, db_path: str = "bot_data.db"):
        self.db_path = db_path
        try:
            self.init_db()
        except Exception as e:
            logger.exception("Failed initializing DB: %s", e)
            # Try to recreate DB if initialization fails
            try:
                if os.path.exists(db_path):
                    os.remove(db_path)
                    logger.info("Removed corrupted database file")
                self.init_db()
            except Exception:
                logger.exception("Failed to recreate DB")

    def _apply_pragmas(self, conn: sqlite3.Connection):
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute("PRAGMA temp_store=MEMORY;")
            conn.execute("PRAGMA cache_size=-1000;")
            conn.execute("PRAGMA mmap_size=268435456;")
        except Exception:
            pass

    def _create_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        self._apply_pragmas(conn)
        return conn

    def get_connection(self) -> sqlite3.Connection:
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

        try:
            _thread_local.conn = self._create_connection()
            return _thread_local.conn
        except Exception as e:
            logger.exception("Failed to create DB connection: %s", e)
            raise

    def close_connection(self):
        conn = getattr(_thread_local, "conn", None)
        if conn:
            try:
                conn.close()
            except Exception:
                logger.exception("Failed to close DB connection")
            _thread_local.conn = None

    def init_db(self):
        with _conn_init_lock:
            conn = self.get_connection()
            cur = conn.cursor()
            
            # Users table
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    phone TEXT,
                    name TEXT,
                    session_data TEXT,
                    is_logged_in INTEGER DEFAULT 0,
                    created_at TEXT DEFAULT (datetime('now')),
                    updated_at TEXT DEFAULT (datetime('now'))
                )
            """
            )

            # Monitoring tasks table
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS monitoring_tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    label TEXT,
                    chat_ids TEXT,
                    settings TEXT,
                    is_active INTEGER DEFAULT 1,
                    created_at TEXT DEFAULT (datetime('now')),
                    updated_at TEXT DEFAULT (datetime('now')),
                    FOREIGN KEY (user_id) REFERENCES users (user_id),
                    UNIQUE(user_id, label)
                )
            """
            )

            # Allowed users table
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS allowed_users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    is_admin INTEGER DEFAULT 0,
                    added_by INTEGER,
                    created_at TEXT DEFAULT (datetime('now'))
                )
            """
            )

            # Message history for duplicate detection - FIXED: Removed inline INDEX
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS message_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    chat_id INTEGER,
                    message_hash TEXT,
                    message_text TEXT,
                    sender_id INTEGER,
                    timestamp TEXT DEFAULT (datetime('now'))
                )
            """
            )

            # Create indexes separately
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_user_chat_hash 
                ON message_history (user_id, chat_id, message_hash)
            """)
            
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_message_history_timestamp 
                ON message_history (timestamp)
            """)
            
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_users_logged_in 
                ON users (is_logged_in)
            """)
            
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_monitoring_tasks_active 
                ON monitoring_tasks (user_id, is_active)
            """)

            conn.commit()
            logger.info("✅ Database initialized successfully")

    def get_user(self, user_id: int) -> Optional[Dict]:
        conn = self.get_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
            row = cur.fetchone()
            if not row:
                return None
            return {
                "user_id": row["user_id"],
                "phone": row["phone"],
                "name": row["name"],
                "session_data": row["session_data"],
                "is_logged_in": row["is_logged_in"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
            }
        except Exception as e:
            logger.exception("Error in get_user for %s: %s", user_id, e)
            raise

    def save_user(
        self,
        user_id: int,
        phone: Optional[str] = None,
        name: Optional[str] = None,
        session_data: Optional[str] = None,
        is_logged_in: bool = False,
    ):
        conn = self.get_connection()
        try:
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
                cur.execute(
                    """
                    INSERT INTO users (user_id, phone, name, session_data, is_logged_in)
                    VALUES (?, ?, ?, ?, ?)
                """,
                    (user_id, phone, name, session_data, 1 if is_logged_in else 0),
                )

            conn.commit()
        except Exception as e:
            logger.exception("Error in save_user for %s: %s", user_id, e)
            raise

    def add_monitoring_task(self, user_id: int, label: str, chat_ids: List[int], settings: Optional[Dict[str, Any]] = None) -> bool:
        conn = self.get_connection()
        try:
            cur = conn.cursor()
            try:
                # Default settings
                if settings is None:
                    settings = {
                        "duplicate_detection": True,
                        "notification_alerts": True,
                        "manual_reply_system": True,
                        "auto_reply_system": False,
                        "outgoing_message_monitoring": True
                    }
                
                cur.execute(
                    """
                    INSERT INTO monitoring_tasks (user_id, label, chat_ids, settings)
                    VALUES (?, ?, ?, ?)
                """,
                    (user_id, label, json.dumps(chat_ids), json.dumps(settings)),
                )
                conn.commit()
                return True
            except sqlite3.IntegrityError:
                return False
        except Exception as e:
            logger.exception("Error in add_monitoring_task for %s: %s", user_id, e)
            raise

    def update_task_settings(self, user_id: int, label: str, settings: Dict[str, Any]) -> bool:
        """Update settings for a specific monitoring task"""
        conn = self.get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE monitoring_tasks 
                SET settings = ?, updated_at = ?
                WHERE user_id = ? AND label = ?
                """,
                (json.dumps(settings), datetime.now().isoformat(), user_id, label),
            )
            updated = cur.rowcount > 0
            conn.commit()
            return updated
        except Exception as e:
            logger.exception("Error in update_task_settings for %s, task %s: %s", user_id, label, e)
            raise

    def remove_monitoring_task(self, user_id: int, label: str) -> bool:
        conn = self.get_connection()
        try:
            cur = conn.cursor()
            cur.execute("DELETE FROM monitoring_tasks WHERE user_id = ? AND label = ?", (user_id, label))
            deleted = cur.rowcount > 0
            conn.commit()
            return deleted
        except Exception as e:
            logger.exception("Error in remove_monitoring_task for %s: %s", user_id, e)
            raise

    def get_user_tasks(self, user_id: int) -> List[Dict]:
        conn = self.get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT id, label, chat_ids, settings, is_active, created_at
                FROM monitoring_tasks
                WHERE user_id = ? AND is_active = 1
                ORDER BY created_at DESC
            """,
                (user_id,),
            )

            tasks = []
            for row in cur.fetchall():
                try:
                    settings_data = json.loads(row["settings"]) if row["settings"] else {}
                except (json.JSONDecodeError, TypeError):
                    settings_data = {}
                    
                tasks.append(
                    {
                        "id": row["id"],
                        "label": row["label"],
                        "chat_ids": json.loads(row["chat_ids"]) if row["chat_ids"] else [],
                        "settings": settings_data,
                        "is_active": row["is_active"],
                        "created_at": row["created_at"],
                    }
                )

            return tasks
        except Exception as e:
            logger.exception("Error in get_user_tasks for %s: %s", user_id, e)
            raise

    def get_all_active_tasks(self) -> List[Dict]:
        conn = self.get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT user_id, id, label, chat_ids, settings
                FROM monitoring_tasks
                WHERE is_active = 1
            """
            )
            tasks = []
            for row in cur.fetchall():
                try:
                    settings_data = json.loads(row["settings"]) if row["settings"] else {}
                except (json.JSONDecodeError, TypeError):
                    settings_data = {}
                    
                tasks.append(
                    {
                        "user_id": row["user_id"],
                        "id": row["id"],
                        "label": row["label"],
                        "chat_ids": json.loads(row["chat_ids"]) if row["chat_ids"] else [],
                        "settings": settings_data,
                    }
                )
            return tasks
        except Exception as e:
            logger.exception("Error in get_all_active_tasks: %s", e)
            raise

    def get_all_logged_in_users(self) -> List[Dict]:
        """Get all users with active sessions"""
        conn = self.get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT user_id, phone, name, session_data, is_logged_in
                FROM users
                WHERE is_logged_in = 1 AND session_data IS NOT NULL AND session_data != ''
                ORDER BY user_id
            """
            )
            
            users = []
            for row in cur.fetchall():
                users.append({
                    "user_id": row["user_id"],
                    "phone": row["phone"],
                    "name": row["name"],
                    "session_data": row["session_data"],
                    "is_logged_in": row["is_logged_in"],
                })
            return users
        except Exception as e:
            logger.exception("Error in get_all_logged_in_users: %s", e)
            raise

    def is_user_allowed(self, user_id: int) -> bool:
        conn = self.get_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT user_id FROM allowed_users WHERE user_id = ?", (user_id,))
            return cur.fetchone() is not None
        except Exception as e:
            logger.exception("Error in is_user_allowed for %s: %s", user_id, e)
            raise

    def is_user_admin(self, user_id: int) -> bool:
        conn = self.get_connection()
        try:
            cur = conn.cursor()
            cur.execute("SELECT is_admin FROM allowed_users WHERE user_id = ?", (user_id,))
            row = cur.fetchone()
            return row is not None and int(row["is_admin"]) == 1
        except Exception as e:
            logger.exception("Error in is_user_admin for %s: %s", user_id, e)
            raise

    def add_allowed_user(self, user_id: int, username: Optional[str] = None, is_admin: bool = False, added_by: Optional[int] = None) -> bool:
        conn = self.get_connection()
        try:
            cur = conn.cursor()
            try:
                cur.execute(
                    """
                    INSERT INTO allowed_users (user_id, username, is_admin, added_by)
                    VALUES (?, ?, ?, ?)
                """,
                    (user_id, username, 1 if is_admin else 0, added_by),
                )
                conn.commit()
                return True
            except sqlite3.IntegrityError:
                return False
        except Exception as e:
            logger.exception("Error in add_allowed_user for %s: %s", user_id, e)
            raise

    def remove_allowed_user(self, user_id: int) -> bool:
        conn = self.get_connection()
        try:
            cur = conn.cursor()
            cur.execute("DELETE FROM allowed_users WHERE user_id = ?", (user_id,))
            deleted = cur.rowcount > 0
            conn.commit()
            return deleted
        except Exception as e:
            logger.exception("Error in remove_allowed_user for %s: %s", user_id, e)
            raise

    def get_all_allowed_users(self) -> List[Dict]:
        conn = self.get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT user_id, username, is_admin, added_by, created_at
                FROM allowed_users
                ORDER BY created_at DESC
            """
            )
            users = []
            for row in cur.fetchall():
                users.append(
                    {
                        "user_id": row["user_id"],
                        "username": row["username"],
                        "is_admin": row["is_admin"],
                        "added_by": row["added_by"],
                        "created_at": row["created_at"],
                    }
                )
            return users
        except Exception as e:
            logger.exception("Error in get_all_allowed_users: %s", e)
            raise

    def add_message_to_history(self, user_id: int, chat_id: int, message_hash: str, message_text: str, sender_id: Optional[int] = None):
        """Add message to history for duplicate detection"""
        conn = self.get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO message_history (user_id, chat_id, message_hash, message_text, sender_id)
                VALUES (?, ?, ?, ?, ?)
            """,
                (user_id, chat_id, message_hash, message_text[:500], sender_id),
            )
            conn.commit()
        except Exception as e:
            logger.exception("Error in add_message_to_history: %s", e)
            raise

    def check_duplicate_message(self, user_id: int, chat_id: int, message_hash: str, time_window_hours: int = 24) -> bool:
        """Check if message is a duplicate within time window"""
        conn = self.get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT COUNT(*) as count FROM message_history 
                WHERE user_id = ? AND chat_id = ? AND message_hash = ?
                AND timestamp >= datetime('now', ?)
                """,
                (user_id, chat_id, message_hash, f'-{time_window_hours} hours'),
            )
            row = cur.fetchone()
            return row["count"] > 0 if row else False
        except Exception as e:
            logger.exception("Error in check_duplicate_message: %s", e)
            raise

    def cleanup_old_messages(self, hours_old: int = 24):
        """Cleanup old messages from history"""
        conn = self.get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                "DELETE FROM message_history WHERE timestamp < datetime('now', ?)",
                (f'-{hours_old} hours',),
            )
            deleted = cur.rowcount
            conn.commit()
            logger.info(f"Cleaned up {deleted} old messages from history")
            return deleted
        except Exception as e:
            logger.exception("Error in cleanup_old_messages: %s", e)
            raise

    def get_db_status(self) -> Dict:
        status = {"path": self.db_path, "exists": False, "size_bytes": None, "user_version": None, "counts": {}}
        try:
            status["exists"] = os.path.exists(self.db_path)
            if status["exists"]:
                status["size_bytes"] = os.path.getsize(self.db_path)
            except Exception:
                logger.exception("Error reading DB file info")

            try:
                conn = self.get_connection()
                try:
                    cur = conn.cursor()
                    try:
                        cur.execute("PRAGMA user_version;")
                        row = cur.fetchone()
                        if row:
                            try:
                                status["user_version"] = int(row[0])
                            except Exception:
                                try:
                                    status["user_version"] = int(row["user_version"])
                                except Exception:
                                    status["user_version"] = None
                    except Exception:
                        status["user_version"] = None

                    for table in ("users", "monitoring_tasks", "allowed_users", "message_history"):
                        try:
                            cur.execute(f"SELECT COUNT(1) as c FROM {table}")
                            crow = cur.fetchone()
                            if crow:
                                try:
                                    cnt = crow["c"]
                                except Exception:
                                    cnt = crow[0]
                                status["counts"][table] = int(cnt)
                            else:
                                status["counts"][table] = 0
                        except Exception:
                            status["counts"][table] = None
                finally:
                    self.close_connection()
            except Exception:
                logger.exception("Error querying DB status")

            return status

    def __del__(self):
        try:
            self.close_connection()
        except Exception:
            pass
