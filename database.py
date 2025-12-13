# database.py (optimized & corrected)
import sqlite3
import json
import threading
from datetime import datetime
from typing import List, Dict, Optional, Any, Set
import os
import logging
import atexit
from collections import defaultdict

logger = logging.getLogger("database")

# Thread-local connection holder
_connection_pool = threading.local()


class Database:
    def __init__(self, db_path: str = "bot_data.db"):
        self.db_path = db_path
        self._init_lock = threading.Lock()
        self._cache_lock = threading.Lock()

        # In-memory caches
        # Keep caches small: only keep logged-in users, allowed users and admins in memory.
        # Tasks are loaded on-demand to avoid large memory usage at startup.
        self._user_cache: Dict[int, Dict] = {}
        self._tasks_cache: Dict[int, List[Dict]] = defaultdict(list)
        self._allowed_users_cache: Set[int] = set()
        self._admin_cache: Set[int] = set()

        try:
            self._init_db()
            self._load_caches()
        except Exception as e:
            logger.exception("Failed initializing DB: %s", e)
            # Try to recreate DB if initialization fails
            try:
                if os.path.exists(db_path):
                    os.remove(db_path)
                    logger.info("Removed corrupted database file")
                self._init_db()
                self._load_caches()
            except Exception:
                logger.exception("Failed to recreate DB")

        atexit.register(self.close_all_connections)

    def _get_connection(self) -> sqlite3.Connection:
        """Get or create a thread-local connection"""
        conn = getattr(_connection_pool, 'connection', None)
        if conn is None:
            # Use check_same_thread=False because connections will be used across threads via threadpool
            conn = sqlite3.connect(self.db_path, timeout=30, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            _connection_pool.connection = conn

            # Optimize connection for performance and lower IO
            try:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=NORMAL")
                conn.execute("PRAGMA cache_size=-10000")  # ~10MB cache (negative means KB)
                conn.execute("PRAGMA mmap_size=268435456")
                conn.execute("PRAGMA busy_timeout=5000")
            except Exception:
                # Not critical if PRAGMAs fail on some SQLite builds
                pass

        return conn

    def close_all_connections(self):
        """Close all database connections"""
        try:
            conn = getattr(_connection_pool, 'connection', None)
            if conn:
                conn.close()
                delattr(_connection_pool, 'connection')
        except Exception:
            pass

    def _init_db(self):
        """Initialize database schema"""
        with self._init_lock:
            conn = self._get_connection()

            # Users table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    phone TEXT,
                    name TEXT,
                    session_data TEXT,
                    is_logged_in INTEGER DEFAULT 0,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Monitoring tasks table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS monitoring_tasks (
                    id INTEGER PRIMARY KEY,
                    user_id INTEGER,
                    label TEXT,
                    chat_ids TEXT,
                    settings TEXT,
                    is_active INTEGER DEFAULT 1,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(user_id, label)
                )
            """)

            # Allowed users table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS allowed_users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    is_admin INTEGER DEFAULT 0,
                    added_by INTEGER,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Create indexes for faster lookups
            conn.execute("CREATE INDEX IF NOT EXISTS idx_users_logged_in ON users(is_logged_in)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_user_active ON monitoring_tasks(user_id, is_active)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_active ON monitoring_tasks(is_active)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_allowed_admins ON allowed_users(is_admin)")

            conn.commit()
            logger.info("✅ Database initialized successfully")

    def _load_caches(self):
        """Load frequently accessed small datasets into memory to reduce DB hits.
        Note: monitoring tasks are intentionally not fully loaded here to avoid large memory usage.
        Tasks are loaded on demand via get_user_tasks / get_all_active_tasks.
        """
        try:
            conn = self._get_connection()

            # Load allowed users and admins
            cursor = conn.execute("SELECT user_id, is_admin FROM allowed_users")
            for row in cursor:
                user_id = int(row['user_id'])
                self._allowed_users_cache.add(user_id)
                if int(row['is_admin']):
                    self._admin_cache.add(user_id)

            # Load logged-in users (small set)
            cursor = conn.execute("SELECT user_id, phone, name, session_data, is_logged_in, created_at, updated_at FROM users WHERE is_logged_in = 1")
            for row in cursor:
                uid = int(row['user_id'])
                entry = {
                    'user_id': uid,
                    'phone': row['phone'],
                    'name': row['name'],
                    'session_data': row['session_data'],
                    'is_logged_in': int(row['is_logged_in']),
                    'created_at': row['created_at'],
                    'updated_at': row['updated_at']
                }
                self._user_cache[uid] = entry

            logger.info(f"✅ Loaded small caches: {len(self._allowed_users_cache)} allowed users, {len(self._user_cache)} logged-in users")
        except Exception as e:
            logger.exception("Error loading caches: %s", e)

    def get_user(self, user_id: int) -> Optional[Dict]:
        """Get user from cache or database"""
        # Check cache first
        if user_id in self._user_cache:
            # return a shallow copy to avoid external mutations
            return self._user_cache[user_id].copy()

        try:
            conn = self._get_connection()
            cursor = conn.execute("SELECT user_id, phone, name, session_data, is_logged_in, created_at, updated_at FROM users WHERE user_id = ?", (user_id,))
            row = cursor.fetchone()

            if row:
                user_data = {
                    'user_id': int(row['user_id']),
                    'phone': row['phone'],
                    'name': row['name'],
                    'session_data': row['session_data'],
                    'is_logged_in': int(row['is_logged_in']),
                    'created_at': row['created_at'],
                    'updated_at': row['updated_at']
                }
                self._user_cache[user_id] = user_data
                return user_data.copy()
            return None
        except Exception as e:
            logger.exception("Error in get_user for %s: %s", user_id, e)
            return None

    def save_user(self, user_id: int, phone: Optional[str] = None, name: Optional[str] = None,
                  session_data: Optional[str] = None, is_logged_in: bool = False):
        """Save user data with cache update"""
        try:
            conn = self._get_connection()
            now = datetime.now().isoformat()

            # Check if user exists
            cursor = conn.execute("SELECT 1 FROM users WHERE user_id = ?", (user_id,))
            exists = cursor.fetchone() is not None

            if exists:
                # Build update query dynamically
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
                params.append(now)

                params.append(user_id)
                query = f"UPDATE users SET {', '.join(updates)} WHERE user_id = ?"
                conn.execute(query, params)
            else:
                conn.execute("""
                    INSERT INTO users (user_id, phone, name, session_data, is_logged_in, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (user_id, phone, name, session_data, 1 if is_logged_in else 0, now, now))

            conn.commit()

            # Update cache
            if user_id in self._user_cache:
                user_data = self._user_cache[user_id]
                if phone is not None:
                    user_data['phone'] = phone
                if name is not None:
                    user_data['name'] = name
                if session_data is not None:
                    user_data['session_data'] = session_data
                user_data['is_logged_in'] = 1 if is_logged_in else 0
                user_data['updated_at'] = now
            else:
                # store minimal user info to cache if logged in
                if is_logged_in:
                    self._user_cache[user_id] = {
                        'user_id': user_id,
                        'phone': phone,
                        'name': name,
                        'session_data': session_data,
                        'is_logged_in': 1 if is_logged_in else 0,
                        'updated_at': now
                    }

        except Exception as e:
            logger.exception("Error in save_user for %s: %s", user_id, e)
            raise

    def add_monitoring_task(self, user_id: int, label: str, chat_ids: List[int],
                           settings: Optional[Dict[str, Any]] = None) -> bool:
        """Add monitoring task with cache update"""
        try:
            conn = self._get_connection()

            if settings is None:
                settings = {
                    "check_duplicate_and_notify": True,
                    "manual_reply_system": True,
                    "auto_reply_system": False,
                    "auto_reply_message": "",
                    "outgoing_message_monitoring": True
                }

            try:
                now = datetime.now().isoformat()
                conn.execute("""
                    INSERT INTO monitoring_tasks (user_id, label, chat_ids, settings, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (user_id, label, json.dumps(chat_ids), json.dumps(settings), now, now))

                # Get the inserted ID
                cursor = conn.execute("SELECT last_insert_rowid() as id")
                row = cursor.fetchone()
                task_id = row['id'] if row else None

                conn.commit()

                # Update cache: append to user's tasks list (kept minimal in memory)
                task = {
                    'id': task_id,
                    'label': label,
                    'chat_ids': chat_ids,
                    'settings': settings,
                    'is_active': 1
                }
                self._tasks_cache[user_id].append(task)

                return True
            except sqlite3.IntegrityError:
                return False
        except Exception as e:
            logger.exception("Error in add_monitoring_task for %s: %s", user_id, e)
            return False

    def update_task_settings(self, user_id: int, label: str, settings: Dict[str, Any]) -> bool:
        """Update task settings with cache synchronization"""
        try:
            conn = self._get_connection()
            now = datetime.now().isoformat()

            conn.execute("""
                UPDATE monitoring_tasks
                SET settings = ?, updated_at = ?
                WHERE user_id = ? AND label = ?
            """, (json.dumps(settings), now, user_id, label))

            updated = conn.total_changes > 0
            conn.commit()

            # Update cache if found
            if updated and user_id in self._tasks_cache:
                for task in self._tasks_cache[user_id]:
                    if task['label'] == label:
                        task['settings'] = settings
                        break

            return updated
        except Exception as e:
            logger.exception("Error in update_task_settings for %s, task %s: %s", user_id, label, e)
            return False

    def remove_monitoring_task(self, user_id: int, label: str) -> bool:
        """Remove monitoring task from cache and database"""
        try:
            conn = self._get_connection()
            conn.execute("DELETE FROM monitoring_tasks WHERE user_id = ? AND label = ?", (user_id, label))
            deleted = conn.total_changes > 0
            conn.commit()

            # Remove from cache
            if deleted and user_id in self._tasks_cache:
                self._tasks_cache[user_id] = [t for t in self._tasks_cache[user_id] if t.get('label') != label]

            return deleted
        except Exception as e:
            logger.exception("Error in remove_monitoring_task for %s: %s", user_id, e)
            return False

    def get_user_tasks(self, user_id: int) -> List[Dict]:
        """Get user tasks from cache or database (loads on demand)"""
        # Return cached copy if present
        if user_id in self._tasks_cache and self._tasks_cache[user_id]:
            return [t.copy() for t in self._tasks_cache[user_id]]

        # Load from database (on demand) to save memory at startup
        try:
            conn = self._get_connection()
            cursor = conn.execute("SELECT id, label, chat_ids, settings, is_active FROM monitoring_tasks WHERE user_id = ? AND is_active = 1 ORDER BY created_at ASC", (user_id,))
            tasks = []
            for row in cursor:
                task = {
                    'id': int(row['id']),
                    'label': row['label'],
                    'chat_ids': json.loads(row['chat_ids']) if row['chat_ids'] else [],
                    'settings': json.loads(row['settings']) if row['settings'] else {},
                    'is_active': int(row['is_active'])
                }
                tasks.append(task)

            # Cache the loaded tasks (lightweight)
            if tasks:
                self._tasks_cache[user_id] = tasks

            return [t.copy() for t in tasks]
        except Exception as e:
            logger.exception("Error in get_user_tasks for %s: %s", user_id, e)
            return []

    def get_all_active_tasks(self) -> List[Dict]:
        """Get all active tasks directly from the database (used during restore)"""
        try:
            conn = self._get_connection()
            cursor = conn.execute("SELECT user_id, id, label, chat_ids, settings FROM monitoring_tasks WHERE is_active = 1")
            tasks = []
            for row in cursor:
                uid = int(row['user_id'])
                task = {
                    'user_id': uid,
                    'id': int(row['id']),
                    'label': row['label'],
                    'chat_ids': json.loads(row['chat_ids']) if row['chat_ids'] else [],
                    'settings': json.loads(row['settings']) if row['settings'] else {}
                }
                tasks.append(task)

                # Also update the lightweight cache incrementally to avoid extra DB calls later
                if uid not in self._tasks_cache or not any(t['id'] == task['id'] for t in self._tasks_cache.get(uid, [])):
                    self._tasks_cache[uid].append({
                        'id': task['id'],
                        'label': task['label'],
                        'chat_ids': task['chat_ids'],
                        'settings': task['settings'],
                        'is_active': 1
                    })

            return tasks
        except Exception as e:
            logger.exception("Error in get_all_active_tasks: %s", e)
            return []

    def get_all_logged_in_users(self) -> List[Dict]:
        """Get all logged-in users from cache"""
        # Cached at startup with only logged-in users to reduce memory usage
        try:
            return [user.copy() for user in self._user_cache.values() if user.get('is_logged_in')]
        except Exception as e:
            logger.exception("Error in get_all_logged_in_users: %s", e)
            return []

    def is_user_allowed(self, user_id: int) -> bool:
        """Check if user is allowed (from cache or DB fallback)"""
        if user_id in self._allowed_users_cache:
            return True

        # Fall back to DB lookup to be accurate
        try:
            conn = self._get_connection()
            cursor = conn.execute("SELECT 1 FROM allowed_users WHERE user_id = ?", (user_id,))
            exists = cursor.fetchone() is not None
            if exists:
                self._allowed_users_cache.add(user_id)
            return exists
        except Exception:
            logger.exception("Error checking is_user_allowed for %s", user_id)
            return False

    def is_user_admin(self, user_id: int) -> bool:
        """Check if user is admin (from cache or DB fallback)"""
        if user_id in self._admin_cache:
            return True

        try:
            conn = self._get_connection()
            cursor = conn.execute("SELECT is_admin FROM allowed_users WHERE user_id = ?", (user_id,))
            row = cursor.fetchone()
            if row and int(row['is_admin']):
                self._admin_cache.add(user_id)
                self._allowed_users_cache.add(user_id)
                return True
            return False
        except Exception:
            logger.exception("Error checking is_user_admin for %s", user_id)
            return False

    def add_allowed_user(self, user_id: int, username: Optional[str] = None,
                         is_admin: bool = False, added_by: Optional[int] = None) -> bool:
        """Add allowed user with cache update"""
        try:
            conn = self._get_connection()

            try:
                now = datetime.now().isoformat()
                conn.execute("""
                    INSERT INTO allowed_users (user_id, username, is_admin, added_by, created_at)
                    VALUES (?, ?, ?, ?, ?)
                """, (user_id, username, 1 if is_admin else 0, added_by, now))

                conn.commit()

                # Update caches
                self._allowed_users_cache.add(user_id)
                if is_admin:
                    self._admin_cache.add(user_id)

                return True
            except sqlite3.IntegrityError:
                return False
        except Exception as e:
            logger.exception("Error in add_allowed_user for %s: %s", user_id, e)
            return False

    def remove_allowed_user(self, user_id: int) -> bool:
        """Remove allowed user from cache and database"""
        try:
            conn = self._get_connection()
            conn.execute("DELETE FROM allowed_users WHERE user_id = ?", (user_id,))
            removed = conn.total_changes > 0
            conn.commit()

            # Remove from caches and remove related in-memory data
            if removed:
                self._allowed_users_cache.discard(user_id)
                self._admin_cache.discard(user_id)
                self._user_cache.pop(user_id, None)
                self._tasks_cache.pop(user_id, None)

            return removed
        except Exception as e:
            logger.exception("Error in remove_allowed_user for %s: %s", user_id, e)
            return False

    def get_all_allowed_users(self) -> List[Dict]:
        """Get all allowed users from database"""
        try:
            conn = self._get_connection()
            cursor = conn.execute("""
                SELECT user_id, username, is_admin, added_by, created_at
                FROM allowed_users
                ORDER BY created_at DESC
            """)

            users = []
            for row in cursor:
                users.append({
                    'user_id': int(row['user_id']),
                    'username': row['username'],
                    'is_admin': int(row['is_admin']),
                    'added_by': row['added_by'],
                    'created_at': row['created_at']
                })
            return users
        except Exception as e:
            logger.exception("Error in get_all_allowed_users: %s", e)
            return []

    def get_db_status(self) -> Dict:
        """Get database status information"""
        status = {
            "path": self.db_path,
            "exists": os.path.exists(self.db_path),
            "cache_counts": {
                "users": len(self._user_cache),
                "tasks": sum(len(tasks) for tasks in self._tasks_cache.values()),
                "allowed_users": len(self._allowed_users_cache),
                "admins": len(self._admin_cache)
            }
        }

        try:
            if status["exists"]:
                status["size_bytes"] = os.path.getsize(self.db_path)

            conn = self._get_connection()
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
            status["tables"] = [row[0] for row in cursor.fetchall()]

        except Exception as e:
            logger.exception("Error getting DB status: %s", e)
            status["error"] = str(e)

        return status

    def close_connection(self):
        """Close the current thread's connection"""
        self.close_all_connections()
