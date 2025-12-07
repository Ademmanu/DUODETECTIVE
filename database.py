import sqlite3
import json
import threading
from datetime import datetime
from typing import List, Dict, Optional, Any
import os
import logging
import hashlib

logger = logging.getLogger("database")

"""
SQLite helper for Duplicate Message Monitor + Manual Reply System

This is based on the FORWARDIFY database structure with added monitoring
tables and helper methods for duplicate detection.
"""

_conn_init_lock = threading.Lock()
_thread_local = threading.local()


class Database:
    def __init__(self, db_path: str = "bot_data.db"):
        self.db_path = db_path
        try:
            self.init_db()
        except Exception:
            logger.exception("Failed initializing DB")

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
                    FOREIGN KEY (user_id) REFERENCES users (user_id),
                    UNIQUE(user_id, label)
                )
            """
            )

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

            # store seen message hashes for duplicate detection
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS seen_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER,
                    message_id INTEGER,
                    message_hash TEXT,
                    content_preview TEXT,
                    seen_at TEXT DEFAULT (datetime('now'))
                )
                """
            )

            conn.commit()

    # ---- User helpers ----
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

    # ---- Allowed users ----
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

    # ---- Monitoring tasks ----
    def add_monitoring_task(self, user_id: int, label: str, chat_ids: List[int], settings: Optional[Dict[str, Any]] = None) -> bool:
        conn = self.get_connection()
        try:
            cur = conn.cursor()
            if settings is None:
                settings = {
                    "duplicate_detection": True,
                    "notify": True,
                    "manual_reply": True,
                    "auto_reply": False,
                    "outgoing": True
                }
            try:
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

    def get_user_monitoring_tasks(self, user_id: int) -> List[Dict]:
        conn = self.get_connection()
        try:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT id, label, chat_ids, settings, is_active, created_at
                FROM monitoring_tasks
                WHERE user_id = ?
                ORDER BY created_at DESC
            """,
                (user_id,),
            )
            tasks = []
            for row in cur.fetchall():
                try:
                    settings = json.loads(row["settings"]) if row["settings"] else {}
                except Exception:
                    settings = {}
                tasks.append({
                    "id": row["id"],
                    "label": row["label"],
                    "chat_ids": json.loads(row["chat_ids"]) if row["chat_ids"] else [],
                    "settings": settings,
                    "is_active": row["is_active"],
                    "created_at": row["created_at"],
                })
            return tasks
        except Exception as e:
            logger.exception("Error in get_user_monitoring_tasks for %s: %s", user_id, e)
            raise

    # ---- Duplicate detection helpers ----
    def _hash_message(self, text: str) -> str:
        h = hashlib.sha256()
        h.update(text.encode("utf-8", errors="ignore"))
        return h.hexdigest()

    def record_message(self, chat_id: int, message_id: int, text: str):
        """Record a message's hash for future duplicate detection"""
        conn = self.get_connection()
        try:
            mh = self._hash_message(text)
            preview = text[:200]
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO seen_messages (chat_id, message_id, message_hash, content_preview)
                VALUES (?, ?, ?, ?)
                """,
                (chat_id, message_id, mh, preview),
            )
            conn.commit()
        except Exception:
            logger.exception("Error recording message hash")

    def find_duplicate(self, chat_id: int, text: str) -> Optional[Dict]:
        """Return previous matching message record in the same chat, or None"""
        conn = self.get_connection()
        try:
            mh = self._hash_message(text)
            cur = conn.cursor()
            cur.execute(
                """
                SELECT id, chat_id, message_id, content_preview, seen_at
                FROM seen_messages
                WHERE chat_id = ? AND message_hash = ?
                ORDER BY id DESC LIMIT 1
                """,
                (chat_id, mh),
            )
            row = cur.fetchone()
            if row:
                return {
                    "id": row["id"],
                    "chat_id": row["chat_id"],
                    "message_id": row["message_id"],
                    "content_preview": row["content_preview"],
                    "seen_at": row["seen_at"],
                }
            return None
        except Exception:
            logger.exception("Error checking for duplicate")
            return None

    # ---- Utility / status ----
    def get_logged_in_users(self, limit: Optional[int] = None) -> List[Dict]:
        conn = self.get_connection()
        try:
            cur = conn.cursor()
            if limit and int(limit) > 0:
                cur.execute(
                    "SELECT user_id, session_data FROM users WHERE is_logged_in = 1 ORDER BY updated_at DESC LIMIT ?",
                    (int(limit),),
                )
            else:
                cur.execute(
                    "SELECT user_id, session_data FROM users WHERE is_logged_in = 1 ORDER BY updated_at DESC"
                )
            rows = cur.fetchall()
            result = []
            for r in rows:
                try:
                    user_id = r["user_id"]
                    session_data = r["session_data"]
                except Exception:
                    user_id, session_data = r[0], r[1]
                result.append({"user_id": user_id, "session_data": session_data})
            return result
        except Exception as e:
            logger.exception("Error fetching logged-in users: %s", e)
            raise

    def get_db_status(self) -> Dict:
        status = {"path": self.db_path, "exists": False, "size_bytes": None, "counts": {}}
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
                for table in ("users", "monitoring_tasks", "allowed_users", "seen_messages"):
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