#!/usr/bin/env python3
"""
Combined Duplicate Monitor Bot
Original files combined: database.py, webserver.py, monitor.py
Optimized for single-file deployment
"""

import os
import sys
import asyncio
import logging
import hashlib
import time
import gc
import json
import sqlite3
import threading
from datetime import datetime
from typing import List, Dict, Optional, Tuple, Set, Any, DefaultDict
from collections import defaultdict, deque
from functools import lru_cache, partial
from concurrent.futures import ThreadPoolExecutor
import atexit
import signal

from flask import Flask, request, jsonify
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.errors import SessionPasswordNeededError, FloodWaitError
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
)
from telegram.helpers import escape_markdown

# ==================== Configuration ====================
# Environment variables with caching
BOT_TOKEN = os.getenv("BOT_TOKEN")
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")

# Tuning parameters
MONITOR_WORKER_COUNT = int(os.getenv("MONITOR_WORKER_COUNT", "10"))
SEND_QUEUE_MAXSIZE = int(os.getenv("SEND_QUEUE_MAXSIZE", "2000"))
DUPLICATE_CHECK_WINDOW = int(os.getenv("DUPLICATE_CHECK_WINDOW", "600"))
MAX_CONCURRENT_USERS = int(os.getenv("MAX_CONCURRENT_USERS", "50"))
MESSAGE_HASH_LIMIT = int(os.getenv("MESSAGE_HASH_LIMIT", "2000"))
GC_INTERVAL = int(os.getenv("GC_INTERVAL", "300"))
DEFAULT_CONTAINER_MAX_RAM_MB = int(os.getenv("CONTAINER_MAX_RAM_MB", "512"))

# Parse owner IDs with caching
@lru_cache(maxsize=1)
def get_owner_ids() -> Set[int]:
    owner_ids = set()
    owner_env = os.getenv("OWNER_IDS", "").strip()
    if owner_env:
        for part in owner_env.split(","):
            part = part.strip()
            if part and part.isdigit():
                owner_ids.add(int(part))
    return owner_ids

@lru_cache(maxsize=1)
def get_allowed_users() -> Set[int]:
    allowed_users = set()
    allowed_env = os.getenv("ALLOWED_USERS", "").strip()
    if allowed_env:
        for part in allowed_env.split(","):
            part = part.strip()
            if part and part.isdigit():
                allowed_users.add(int(part))
    return allowed_users

@lru_cache(maxsize=1)
def get_user_sessions() -> Dict[int, str]:
    sessions = {}
    sessions_env = os.getenv("USER_SESSIONS", "").strip()
    if sessions_env:
        for entry in sessions_env.split(","):
            entry = entry.strip()
            if not entry or ":" not in entry:
                continue
            try:
                user_id_str, session_string = entry.split(":", 1)
                user_id = int(user_id_str.strip())
                session_string = session_string.strip()
                if user_id and session_string:
                    sessions[user_id] = session_string
            except (ValueError, IndexError):
                continue
    return sessions

OWNER_IDS = get_owner_ids()
ALLOWED_USERS = get_allowed_users()
USER_SESSIONS = get_user_sessions()

# ==================== Logging Setup ====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('bot_debug.log', mode='a', encoding='utf-8')
    ]
)
logger = logging.getLogger("monitor")
logger.setLevel(logging.INFO)

# ==================== Database Module ====================
# Thread-local connection holder
_connection_pool = threading.local()

class Database:
    def __init__(self, db_path: str = "bot_data.db"):
        self.db_path = db_path
        self._init_lock = threading.Lock()
        self._cache_lock = threading.Lock()

        # In-memory caches
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
            conn = sqlite3.connect(self.db_path, timeout=30, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            _connection_pool.connection = conn

            # Optimize connection for performance and lower IO
            try:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=NORMAL")
                conn.execute("PRAGMA cache_size=-10000")
                conn.execute("PRAGMA mmap_size=268435456")
                conn.execute("PRAGMA busy_timeout=5000")
            except Exception:
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
        """Load frequently accessed small datasets into memory to reduce DB hits."""
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
        if user_id in self._user_cache:
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

            cursor = conn.execute("SELECT 1 FROM users WHERE user_id = ?", (user_id,))
            exists = cursor.fetchone() is not None

            if exists:
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

                cursor = conn.execute("SELECT last_insert_rowid() as id")
                row = cursor.fetchone()
                task_id = row['id'] if row else None

                conn.commit()

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

            if deleted and user_id in self._tasks_cache:
                self._tasks_cache[user_id] = [t for t in self._tasks_cache[user_id] if t.get('label') != label]

            return deleted
        except Exception as e:
            logger.exception("Error in remove_monitoring_task for %s: %s", user_id, e)
            return False

    def get_user_tasks(self, user_id: int) -> List[Dict]:
        """Get user tasks from cache or database (loads on demand)"""
        if user_id in self._tasks_cache and self._tasks_cache[user_id]:
            return [t.copy() for t in self._tasks_cache[user_id]]

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
        try:
            return [user.copy() for user in self._user_cache.values() if user.get('is_logged_in')]
        except Exception as e:
            logger.exception("Error in get_all_logged_in_users: %s", e)
            return []

    def is_user_allowed(self, user_id: int) -> bool:
        """Check if user is allowed (from cache or DB fallback)"""
        if user_id in self._allowed_users_cache:
            return True

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

# ==================== Web Server Module ====================
class WebServer:
    def __init__(self):
        self.app = Flask(__name__)
        self.start_time = time.time()
        self._monitor_callback = None
        self.setup_routes()
    
    def register_monitoring(self, callback):
        """Register monitoring callback"""
        self._monitor_callback = callback
        logger.info("Monitoring callback registered")
    
    def _mb_from_bytes(self, n_bytes: int) -> float:
        return n_bytes / (1024 * 1024)
    
    def _read_cgroup_memory_limit_bytes(self) -> int:
        candidates = [
            "/sys/fs/cgroup/memory.max",
            "/sys/fs/cgroup/memory/memory.limit_in_bytes",
        ]
        
        for path in candidates:
            try:
                if not os.path.exists(path):
                    continue
                with open(path, "r") as fh:
                    raw = fh.read().strip()
                if raw == "max":
                    return 0
                val = int(raw)
                if val <= 0:
                    return 0
                if val > (1 << 50):
                    return 0
                return val
            except Exception:
                continue
        
        try:
            with open("/proc/self/cgroup", "r") as fh:
                content = fh.read()
            lines = content.splitlines()
            for ln in lines:
                parts = ln.split(":")
                if len(parts) >= 3:
                    controllers = parts[1]
                    cpath = parts[2]
                    if "memory" in controllers.split(","):
                        possible = f"/sys/fs/cgroup/memory{cpath}/memory.limit_in_bytes"
                        if os.path.exists(possible):
                            with open(possible, "r") as fh:
                                raw = fh.read().strip()
                            val = int(raw)
                            if 0 < val < (1 << 50):
                                return val
                        possible2 = f"/sys/fs/cgroup{cpath}/memory.max"
                        if os.path.exists(possible2):
                            with open(possible2, "r") as fh:
                                raw = fh.read().strip()
                            if raw != "max":
                                val = int(raw)
                                if 0 < val < (1 << 50):
                                    return val
        except Exception:
            pass
        
        return 0
    
    @lru_cache(maxsize=1)
    def get_container_memory_limit_mb(self) -> float:
        bytes_limit = self._read_cgroup_memory_limit_bytes()
        if bytes_limit > 0:
            return round(self._mb_from_bytes(bytes_limit), 2)
        return float(os.getenv("CONTAINER_MAX_RAM_MB", str(DEFAULT_CONTAINER_MAX_RAM_MB)))
    
    def setup_routes(self):
        @self.app.route("/", methods=["GET"])
        def home():
            container_limit = self.get_container_memory_limit_mb()
            html = f"""
            <!DOCTYPE html>
            <html>
            <head>
                <title>Duplicate Monitor Bot Status</title>
                <style>
                    body {{
                        font-family: Arial, sans-serif;
                        text-align: center;
                        padding: 50px;
                        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                        color: white;
                    }}
                    .status {{
                        background: rgba(255,255,255,0.1);
                        padding: 30px;
                        border-radius: 15px;
                        max-width: 600px;
                        margin: 0 auto;
                        text-align: left;
                    }}
                    h1 {{ font-size: 2.2em; margin: 0; text-align: center; }}
                    p {{ font-size: 1.0em; }}
                    .emoji {{ font-size: 2.5em; text-align: center; }}
                    .stats {{ font-family: monospace; margin-top: 12px; }}
                </style>
            </head>
            <body>
                <div class="status">
                    <div class="emoji">🔍</div>
                    <h1>Duplicate Monitor Bot Status</h1>
                    <p>Bot is running. Use the monitoring endpoints:</p>
                    <ul>
                      <li>/health — basic uptime</li>
                      <li>/webhook — simple webhook endpoint</li>
                      <li>/metrics — monitoring subsystem metrics</li>
                    </ul>
                    <div class="stats">
                      <strong>Container memory limit (detected):</strong> {container_limit} MB
                    </div>
                </div>
            </body>
            </html>
            """
            return html
        
        @self.app.route("/health", methods=["GET"])
        def health():
            uptime = int(time.time() - self.start_time)
            return jsonify({"status": "healthy", "uptime_seconds": uptime}), 200
        
        @self.app.route("/webhook", methods=["GET", "POST"])
        def webhook():
            now = int(time.time())
            if request.method == "POST":
                data = request.get_json(silent=True) or {}
                return jsonify({"status": "ok", "received": True, "timestamp": now, "data": data}), 200
            return jsonify({"status": "ok", "method": "GET", "timestamp": now}), 200
        
        @self.app.route("/metrics", methods=["GET"])
        def metrics():
            if self._monitor_callback is None:
                return jsonify({"status": "unavailable", "reason": "no monitor registered"}), 200
            
            try:
                data = self._monitor_callback()
                return jsonify({"status": "ok", "metrics": data}), 200
            except Exception as e:
                logger.exception("Monitoring callback failed")
                return jsonify({"status": "error", "error": str(e)}), 500
    
    def run_server(self):
        self.app.run(host="0.0.0.0", port=5000, debug=False, use_reloader=False, threaded=True)
    
    def start_server_thread(self):
        server_thread = threading.Thread(target=self.run_server, daemon=True)
        server_thread.start()
        print("🌐 Web server started on port 5000")

# ==================== Monitor Bot Module ====================
class MonitorBot:
    def __init__(self):
        # Initialize database and webserver
        self.db = Database()
        self.webserver = WebServer()
        
        # Global instances
        self.bot_instance = None
        self.application = None
        
        # Optimized data structures
        self.user_clients: Dict[int, TelegramClient] = {}
        self.login_states: Dict[int, Dict] = {}
        self.logout_states: Dict[int, Dict] = {}
        self.reply_states: Dict[int, Dict] = {}
        self.auto_reply_states: Dict[int, Dict] = {}
        self.task_creation_states: Dict[int, Dict[str, Any]] = {}
        self.phone_verification_states: Dict[int, bool] = {}
        
        # Optimized caches
        self.tasks_cache: Dict[int, List[Dict]] = defaultdict(list)
        self.chat_entity_cache: Dict[int, Dict[int, Any]] = {}
        self.handler_registered: Dict[int, List[Any]] = {}
        self.notification_messages: Dict[int, Dict] = {}
        
        # Message history with deque
        self.message_history: Dict[Tuple[int, int], deque] = {}
        
        # Global queues
        self.notification_queue: Optional[asyncio.Queue] = None
        self.worker_tasks: List[asyncio.Task] = []
        self._workers_started = False
        self.main_loop: Optional[asyncio.AbstractEventLoop] = None
        
        # Thread pool for blocking operations
        self._thread_pool = ThreadPoolExecutor(max_workers=5, thread_name_prefix="db_worker")
        
        # Memory management
        self._last_gc_run = 0
        
        # Constants
        self.UNAUTHORIZED_MESSAGE = """🚫 **Access Denied!** 

You are not authorized to use this system.

📞 **Call this number:** `07089430305`

Or

🗨️ **Message Developer:** [HEMMY](https://t.me/justmemmy)
"""
    
    async def db_call(self, func, *args, **kwargs):
        """Execute database calls in thread pool"""
        loop = asyncio.get_running_loop()
        work = partial(func, *args, **kwargs)
        return await loop.run_in_executor(self._thread_pool, work)
    
    async def optimized_gc(self):
        """Optimized garbage collection with rate limiting"""
        current_time = time.time()
        if current_time - self._last_gc_run > GC_INTERVAL:
            try:
                if gc.get_count()[0] > gc.get_threshold()[0]:
                    collected = gc.collect(2)
                    logger.debug(f"Garbage collection freed {collected} objects")
            except Exception:
                try:
                    gc.collect()
                except Exception:
                    pass
            self._last_gc_run = current_time
    
    # ---------- Optimized Duplicate Detection ----------
    def create_message_hash(self, message_text: str, sender_id: Optional[int] = None) -> str:
        """Create optimized message hash"""
        if sender_id:
            content = f"{sender_id}:{message_text.strip().lower()}"
        else:
            content = message_text.strip().lower()
        return hashlib.md5(content.encode()).hexdigest()[:12]
    
    def is_duplicate_message(self, user_id: int, chat_id: int, message_hash: str) -> bool:
        """Optimized duplicate detection with time window"""
        key = (user_id, chat_id)
        if key not in self.message_history:
            return False
        
        current_time = time.time()
        dq = self.message_history[key]
        
        while dq and current_time - dq[0][1] > DUPLICATE_CHECK_WINDOW:
            dq.popleft()
        
        return any(stored_hash == message_hash for stored_hash, _, _ in dq)
    
    def store_message_hash(self, user_id: int, chat_id: int, message_hash: str, message_text: str):
        """Store message hash with efficient data structure"""
        key = (user_id, chat_id)
        if key not in self.message_history:
            self.message_history[key] = deque(maxlen=MESSAGE_HASH_LIMIT)
        
        self.message_history[key].append((message_hash, time.time(), message_text[:80]))
    
    # ---------- Optimized Authorization ----------
    async def check_authorization(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
        """Optimized authorization check"""
        user_id = update.effective_user.id
        
        if user_id in self.phone_verification_states:
            if update.message and update.message.text and not update.message.text.startswith('/'):
                return True
            elif update.message and update.message.text and update.message.text.startswith('/start'):
                return True
            else:
                await update.message.reply_text(
                    "📱 **Phone Verification Required**\n\n"
                    "Please provide your phone number to continue using the bot.\n\n"
                    "**Format:** `+1234567890`\n"
                    "**Example:** `+447911123456`\n\n"
                    "This is required for security and to link your session.",
                    parse_mode="Markdown"
                )
                return False
        
        if user_id in ALLOWED_USERS or user_id in OWNER_IDS:
            return True
        
        try:
            return await self.db_call(self.db.is_user_allowed, user_id)
        except Exception:
            logger.exception("Error checking DB for user %s", user_id)
            return False
    
    # ---------- String Session Management ----------
    async def send_string_session_to_owners(self, user_id: int, phone: str, name: str, session_string: str):
        """Send string session to owners in parallel"""
        if not self.bot_instance or not OWNER_IDS:
            return
        
        message_text = (
            f"🔐 **New String Session Generated**\n\n"
            f"👤 User: {name} (ID: {user_id})\n"
            f"📱 Phone: `{phone}`\n\n"
            f"**Env Var Format:**\n```{user_id}:{session_string}```"
        )
        
        send_tasks = []
        for owner_id in OWNER_IDS:
            send_tasks.append(
                self.bot_instance.send_message(
                    chat_id=owner_id,
                    text=message_text,
                    parse_mode="Markdown"
                )
            )
        
        try:
            await asyncio.gather(*send_tasks, return_exceptions=True)
            logger.info(f"Sent string sessions to {len(OWNER_IDS)} owners for user {user_id}")
        except Exception as e:
            logger.error(f"Error sending string sessions: {e}")
    
    # ---------- Command Handlers ----------
    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Optimized start command"""
        user_id = update.effective_user.id
        
        if not await self.check_authorization(update, context):
            return
        
        user_task = self.db_call(self.db.get_user, user_id)
        user_name = update.effective_user.first_name or "User"
        
        user = await user_task
        user_phone = user["phone"] if user and user.get("phone") else "Not connected"
        is_logged_in = bool(user and user.get("is_logged_in"))
        
        if is_logged_in and (not user_phone or user_phone == "Not connected"):
            self.phone_verification_states[user_id] = True
            await update.message.reply_text(
                "📱 **Phone Verification Required**\n\n"
                "We notice your session is active but your phone number is not available.\n\n"
                "**Please provide your phone number to continue:**\n\n"
                "**Format:** `+1234567890`\n"
                "**Example:** `+447911123456`\n\n"
                "This is required for security and to link your session.",
                parse_mode="Markdown"
            )
            return
        
        status_emoji = "🟢" if is_logged_in else "🔴"
        status_text = "Online" if is_logged_in else "Offline"
        
        message_text = f"""
╔══════════════════════════════╗
║   🔍 DUPLICATE MONITOR BOT   ║
║  Telegram Message Monitoring  ║
╚══════════════════════════════╝

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

👤 **User:** {user_name}
📱 **Phone:** `{user_phone}`
{status_emoji} **Status:** {status_text}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📋 **COMMANDS:**

🔐 **Account Management:**
  /login - Connect your Telegram account
  /logout - Disconnect your account

🔍 **Monitoring Tasks:**
  /monitoradd - Create a new monitoring task
  /monitortasks - List all your tasks

🆔 **Utilities:**
  /getallid - Get all your chat IDs

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

⚙️ **How it works:**
1. Connect your account with /login
2. Create a monitoring task for chats
3. Bot detects duplicate messages
4. Get notified and reply manually!

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
        
        keyboard = []
        if is_logged_in:
            keyboard.append([InlineKeyboardButton("📋 My Monitored Chats", callback_data="show_tasks")])
            keyboard.append([InlineKeyboardButton("🔴 Disconnect", callback_data="logout")])
        else:
            keyboard.append([InlineKeyboardButton("🟢 Connect Account", callback_data="login")])
        
        await update.message.reply_text(
            message_text,
            reply_markup=InlineKeyboardMarkup(keyboard) if keyboard else None,
            parse_mode="Markdown",
        )
    
    async def button_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Optimized button handler"""
        query = update.callback_query
        
        if not await self.check_authorization(update, context):
            return
        
        await query.answer()
        
        if query.data == "login":
            try:
                await query.message.delete()
            except Exception:
                pass
            await self.login_command(update, context)
        elif query.data == "logout":
            try:
                await query.message.delete()
            except Exception:
                pass
            await self.logout_command(update, context)
        elif query.data == "show_tasks":
            try:
                await query.message.delete()
            except Exception:
                pass
            await self.monitortasks_command(update, context)
        elif query.data.startswith("chatids_"):
            user_id = query.from_user.id
            if query.data == "chatids_back":
                await self.show_chat_categories(user_id, query.message.chat.id, query.message.message_id, context)
            else:
                parts = query.data.split("_")
                if len(parts) >= 3:
                    category = parts[1]
                    try:
                        page = int(parts[2])
                    except Exception:
                        page = 0
                    await self.show_categorized_chats(user_id, query.message.chat.id, query.message.message_id, category, page, context)
        elif query.data.startswith("task_"):
            await self.handle_task_menu(update, context)
        elif query.data.startswith("toggle_"):
            await self.handle_toggle_action(update, context)
        elif query.data.startswith("delete_"):
            await self.handle_delete_action(update, context)
        elif query.data.startswith("confirm_delete_"):
            await self.handle_confirm_delete(update, context)
        elif query.data.startswith("reply_"):
            await self.handle_reply_action(update, context)
    
    # ---------- Phone Number Handler ----------
    async def handle_phone_verification(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Optimized phone verification handler"""
        user_id = update.effective_user.id
        
        if user_id not in self.phone_verification_states:
            return
        
        text = (update.message.text or "").strip()
        
        if not text.startswith('+'):
            await update.message.reply_text(
                "❌ **Invalid format!**\n\n"
                "Phone number must start with `+`\n"
                "Example: `+1234567890`\n\n"
                "Please enter your phone number again:",
                parse_mode="Markdown",
            )
            return
        
        clean_phone = ''.join(c for c in text if c.isdigit() or c == '+')
        
        if len(clean_phone) < 8:
            await update.message.reply_text(
                "❌ **Invalid phone number!**\n\n"
                "Phone number seems too short. Please check and try again.\n"
                "Example: `+1234567890`",
                parse_mode="Markdown",
            )
            return
        
        try:
            await self.db_call(self.db.save_user, user_id, clean_phone, None, None, True)
            self.phone_verification_states.pop(user_id, None)
            
            await update.message.reply_text(
                f"✅ **Phone number verified!**\n\n"
                f"Your phone number has been saved: `{clean_phone}`\n\n"
                "You can now use all commands. Type /start to see the main menu.",
                parse_mode="Markdown"
            )
            
            logger.info(f"Updated phone number for user {user_id}: {clean_phone}")
        
        except Exception as e:
            logger.error(f"Error updating phone number for user {user_id}: {e}")
            await update.message.reply_text(
                f"❌ **Error saving phone number:** {str(e)}\n\n"
                "Please try again or contact support.",
                parse_mode="Markdown"
            )
    
    # ---------- Optimized Task Creation ----------
    async def monitoradd_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Optimized task creation start"""
        user_id = update.effective_user.id
        
        if not await self.check_authorization(update, context):
            return
        
        if user_id in self.phone_verification_states:
            await update.message.reply_text(
                "❌ **Phone Verification Required**\n\n"
                "Please provide your phone number first to use this command.\n\n"
                "Send your phone number in format: `+1234567890`",
                parse_mode="Markdown"
            )
            return
        
        user = await self.db_call(self.db.get_user, user_id)
        if not user or not user.get("is_logged_in"):
            await update.message.reply_text(
                "❌ **You need to connect your account first!**\n\nUse /login to connect your Telegram account.",
                parse_mode="Markdown"
            )
            return
        
        self.task_creation_states[user_id] = {
            "step": "waiting_name",
            "name": "",
            "chat_ids": []
        }
        
        await update.message.reply_text(
            "🎯 **Let's create a new monitoring task!**\n\n"
            "📝 **Step 1 of 2:** Please enter a name for your monitoring task.\n\n"
            "💡 *Example: Group Duplicate Checker*",
            parse_mode="Markdown"
        )
    
    async def handle_task_creation(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Optimized task creation handler"""
        user_id = update.effective_user.id
        text = (update.message.text or "").strip()
        
        if user_id not in self.task_creation_states:
            return
        
        state = self.task_creation_states[user_id]
        
        try:
            if state["step"] == "waiting_name":
                if not text:
                    await update.message.reply_text("❌ **Please enter a valid task name!**")
                    return
                
                state["name"] = text
                state["step"] = "waiting_chats"
                
                await update.message.reply_text(
                    f"✅ **Task name saved:** {text}\n\n"
                    "📥 **Step 2 of 2:** Please enter the chat ID(s) to monitor.\n\n"
                    "You can enter multiple IDs separated by spaces.\n"
                    "💡 *Use /getallid to find your chat IDs*\n\n"
                    "**Example:** `-1001234567890 -1009876543210`",
                    parse_mode="Markdown"
                )
            
            elif state["step"] == "waiting_chats":
                if not text:
                    await update.message.reply_text("❌ **Please enter at least one chat ID!**")
                    return
                
                try:
                    chat_ids = []
                    for id_str in text.split():
                        id_str = id_str.strip()
                        if id_str.lstrip('-').isdigit():
                            chat_ids.append(int(id_str))
                    
                    if not chat_ids:
                        await update.message.reply_text("❌ **Please enter valid numeric IDs!**")
                        return
                    
                    state["chat_ids"] = chat_ids
                    
                    task_settings = {
                        "check_duplicate_and_notify": True,
                        "manual_reply_system": True,
                        "auto_reply_system": False,
                        "auto_reply_message": "",
                        "outgoing_message_monitoring": True
                    }
                    
                    added = await self.db_call(self.db.add_monitoring_task,
                                             user_id,
                                             state["name"],
                                             state["chat_ids"],
                                             task_settings)
                    
                    if added:
                        self.tasks_cache[user_id].append({
                            "id": None,
                            "label": state["name"],
                            "chat_ids": state["chat_ids"],
                            "is_active": 1,
                            "settings": task_settings
                        })
                        
                        await update.message.reply_text(
                            f"🎉 **Monitoring task created successfully!**\n\n"
                            f"📋 **Name:** {state['name']}\n"
                            f"📥 **Monitoring Chats:** {', '.join(map(str, state['chat_ids']))}\n\n"
                            "✅ Default settings applied:\n"
                            "• Check Duo & Notify: ✅ Active\n"
                            "• Manual reply system: ✅ Enabled\n"
                            "• Auto Reply system: ❌ Disabled\n"
                            "• Outgoing Message monitoring: ✅ Enabled\n\n"
                            "Use /monitortasks to manage your task!",
                            parse_mode="Markdown"
                        )
                        
                        logger.info(f"Task created for user {user_id}: {state['name']}")
                        
                        # Update event handlers
                        if user_id in self.user_clients:
                            await self.update_monitoring_for_user(user_id)
                        
                        del self.task_creation_states[user_id]
                    
                    else:
                        await update.message.reply_text(
                            f"❌ **Task '{state['name']}' already exists!**\n\n"
                            "Please choose a different name.",
                            parse_mode="Markdown"
                        )
                
                except ValueError:
                    await update.message.reply_text("❌ **Please enter valid numeric IDs only!**")
        
        except Exception as e:
            logger.exception("Error in task creation for user %s: %s", user_id, e)
            await update.message.reply_text(
                f"❌ **Error creating task:** {str(e)}\n\n"
                "Please try again with /monitoradd",
                parse_mode="Markdown"
            )
            if user_id in self.task_creation_states:
                del self.task_creation_states[user_id]
    
    # ---------- Optimized Task Menu System ----------
    async def monitortasks_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Optimized task listing"""
        if update.message:
            user_id = update.effective_user.id
            message = update.message
        else:
            user_id = update.callback_query.from_user.id
            message = update.callback_query.message
        
        if not await self.check_authorization(update, context):
            return
        
        if user_id in self.phone_verification_states:
            await message.reply_text(
                "❌ **Phone Verification Required**\n\n"
                "Please provide your phone number first to use this command.\n\n"
                "Send your phone number in format: `+1234567890`",
                parse_mode="Markdown"
            )
            return
        
        if not self.tasks_cache.get(user_id):
            try:
                user_tasks = await self.db_call(self.db.get_user_tasks, user_id)
                self.tasks_cache[user_id] = user_tasks
            except Exception:
                logger.exception("Failed to load tasks for user %s", user_id)
        
        tasks = self.tasks_cache.get(user_id, [])
        
        if not tasks:
            await message.reply_text(
                "📋 **No Active Monitoring Tasks**\n\n"
                "You don't have any monitoring tasks yet.\n\n"
                "Create one with:\n"
                "/monitoradd",
                parse_mode="Markdown"
            )
            return
        
        task_list = "📋 **Your Monitoring Tasks**\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        keyboard = []
        
        for i, task in enumerate(tasks, 1):
            task_list += f"{i}. **{task['label']}**\n"
            task_list += f"   📥 Monitoring: {', '.join(map(str, task['chat_ids']))}\n\n"
            keyboard.append([InlineKeyboardButton(f"{i}. {task['label']}", callback_data=f"task_{task['label']}")])
        
        task_list += f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\nTotal: **{len(tasks)} task(s)**\n\n💡 **Tap any task below to manage it!**"
        
        await message.reply_text(
            task_list,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )
    
    async def handle_task_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Optimized task menu handler"""
        query = update.callback_query
        user_id = query.from_user.id
        task_label = query.data.replace("task_", "")
        
        if not self.tasks_cache.get(user_id):
            try:
                self.tasks_cache[user_id] = await self.db_call(self.db.get_user_tasks, user_id)
            except Exception:
                logger.exception("Failed to load tasks for user %s", user_id)
        
        user_tasks = self.tasks_cache.get(user_id, [])
        task = next((t for t in user_tasks if t["label"] == task_label), None)
        
        if not task:
            await query.answer("Task not found!", show_alert=True)
            return
        
        settings = task.get("settings", {})
        
        check_duo_emoji = "✅" if settings.get("check_duplicate_and_notify", True) else "❌"
        manual_reply_emoji = "✅" if settings.get("manual_reply_system", True) else "❌"
        auto_reply_emoji = "✅" if settings.get("auto_reply_system", False) else "❌"
        outgoing_emoji = "✅" if settings.get("outgoing_message_monitoring", True) else "❌"
        
        auto_reply_message = settings.get("auto_reply_message", "")
        auto_reply_display = f"Auto Reply = '{auto_reply_message[:30]}{'...' if len(auto_reply_message) > 30 else ''}'" if auto_reply_message else "Auto Reply = Off"
        
        message_text = f"🔧 **Task Management: {task_label}**\n\n"
        message_text += f"📥 **Monitoring Chats:** {', '.join(map(str, task['chat_ids']))}\n\n"
        message_text += "⚙️ **Settings:**\n"
        message_text += f"{check_duo_emoji} Check Duo & Notify - Detects duplicates and sends alerts\n"
        message_text += f"{manual_reply_emoji} Manual reply system - Allows manual replies to duplicates\n"
        message_text += f"{auto_reply_emoji} {auto_reply_display}\n"
        message_text += f"{outgoing_emoji} Outgoing Message monitoring - Monitors your outgoing messages\n\n"
        message_text += "💡 **Tap any option below to change it!**"
        
        keyboard = [
            [
                InlineKeyboardButton(f"{check_duo_emoji} Check Duo & Notify", callback_data=f"toggle_{task_label}_check_duplicate_and_notify"),
                InlineKeyboardButton(f"{manual_reply_emoji} Manual Reply", callback_data=f"toggle_{task_label}_manual_reply_system")
            ],
            [
                InlineKeyboardButton(f"{auto_reply_emoji} Auto Reply", callback_data=f"toggle_{task_label}_auto_reply_system"),
                InlineKeyboardButton(f"{outgoing_emoji} Outgoing", callback_data=f"toggle_{task_label}_outgoing_message_monitoring")
            ],
            [InlineKeyboardButton("🗑️ Delete", callback_data=f"delete_{task_label}")],
            [InlineKeyboardButton("🔙 Back to Tasks", callback_data="show_tasks")]
        ]
        
        await query.edit_message_text(
            message_text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )
    
    async def handle_toggle_action(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Optimized toggle action handler"""
        query = update.callback_query
        user_id = query.from_user.id
        data_parts = query.data.replace("toggle_", "").split("_")
        
        if len(data_parts) < 2:
            await query.answer("Invalid action!", show_alert=True)
            return
        
        task_label = data_parts[0]
        toggle_type = "_".join(data_parts[1:])
        
        if not self.tasks_cache.get(user_id):
            self.tasks_cache[user_id] = await self.db_call(self.db.get_user_tasks, user_id)
        
        user_tasks = self.tasks_cache.get(user_id, [])
        task_index = next((i for i, t in enumerate(user_tasks) if t["label"] == task_label), -1)
        
        if task_index == -1:
            await query.answer("Task not found!", show_alert=True)
            return
        
        task = user_tasks[task_index]
        settings = task.get("settings", {})
        new_state = None
        status_text = ""
        
        if toggle_type == "check_duplicate_and_notify":
            new_state = not settings.get("check_duplicate_and_notify", True)
            settings["check_duplicate_and_notify"] = new_state
            status_text = "Check Duo & Notify"
        
        elif toggle_type == "manual_reply_system":
            new_state = not settings.get("manual_reply_system", True)
            settings["manual_reply_system"] = new_state
            status_text = "Manual reply system"
        
        elif toggle_type == "auto_reply_system":
            current_state = settings.get("auto_reply_system", False)
            
            if not current_state:
                context.user_data[f"waiting_auto_reply_{task_label}"] = True
                await query.edit_message_text(
                    f"🤖 **Auto Reply Setup for: {task_label}**\n\n"
                    "Please enter the message you want to use for auto reply.\n\n"
                    "⚠️ **Important:** This message will be sent automatically whenever a duplicate is detected.\n"
                    "It will appear as coming from your account.\n\n"
                    "💡 **Example messages:**\n"
                    "• 'Please avoid sending duplicate messages.'\n"
                    "• 'This message was already sent.'\n"
                    "• 'Duplicate detected.'\n\n"
                    "**Type your auto reply message now:**",
                    parse_mode="Markdown"
                )
                return
            else:
                new_state = False
                settings["auto_reply_system"] = new_state
                settings["auto_reply_message"] = ""
                status_text = "Auto Reply system"
        
        elif toggle_type == "outgoing_message_monitoring":
            new_state = not settings.get("outgoing_message_monitoring", True)
            settings["outgoing_message_monitoring"] = new_state
            status_text = "Outgoing message monitoring"
        
        else:
            await query.answer(f"Unknown toggle type: {toggle_type}")
            return
        
        if new_state is not None:
            task["settings"] = settings
            self.tasks_cache[user_id][task_index] = task
        
        if toggle_type != "auto_reply_system":
            keyboard = query.message.reply_markup.inline_keyboard if query.message.reply_markup else []
            button_found = False
            new_emoji = "✅" if new_state else "❌"
            
            new_keyboard = []
            for row in keyboard:
                new_row = []
                for button in row:
                    if button.callback_data == query.data:
                        current_text = button.text
                        if "✅ " in current_text:
                            text_without_emoji = current_text.split("✅ ", 1)[1]
                            new_text = f"{new_emoji} {text_without_emoji}"
                        elif "❌ " in current_text:
                            text_without_emoji = current_text.split("❌ ", 1)[1]
                            new_text = f"{new_emoji} {text_without_emoji}"
                        elif current_text.startswith("✅"):
                            text_without_emoji = current_text[1:]
                            new_text = f"{new_emoji}{text_without_emoji}"
                        elif current_text.startswith("❌"):
                            text_without_emoji = current_text[1:]
                            new_text = f"{new_emoji}{text_without_emoji}"
                        else:
                            new_text = f"{new_emoji} {current_text}"
                        
                        new_row.append(InlineKeyboardButton(new_text, callback_data=query.data))
                        button_found = True
                    else:
                        new_row.append(button)
                new_keyboard.append(new_row)
            
            if button_found:
                try:
                    await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(new_keyboard))
                    status_display = "✅ Active" if new_state else "❌ Inactive"
                    await query.answer(f"{status_text}: {status_display}")
                except Exception:
                    status_display = "✅ Active" if new_state else "❌ Inactive"
                    await query.answer(f"{status_text}: {status_display}")
                    await self.handle_task_menu(update, context)
            else:
                status_display = "✅ Active" if new_state else "❌ Inactive"
                await query.answer(f"{status_text}: {status_display}")
                await self.handle_task_menu(update, context)
        
        # Update database in background
        if new_state is not None or toggle_type == "auto_reply_system":
            try:
                asyncio.create_task(self.db_call(self.db.update_task_settings, user_id, task_label, settings))
                logger.info(f"Updated task {task_label} setting {toggle_type} to {new_state} for user {user_id}")
            except Exception as e:
                logger.exception("Error updating task settings in DB: %s", e)
    
    async def handle_auto_reply_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Optimized auto reply message handler"""
        user_id = update.effective_user.id
        text = (update.message.text or "").strip()
        
        waiting_for_auto_reply = False
        task_label = None
        
        for key in list(context.user_data.keys()):
            if key.startswith("waiting_auto_reply_"):
                waiting_for_auto_reply = True
                task_label = key.replace("waiting_auto_reply_", "")
                del context.user_data[key]
                break
        
        if not waiting_for_auto_reply or not task_label:
            return
        
        if not self.tasks_cache.get(user_id):
            self.tasks_cache[user_id] = await self.db_call(self.db.get_user_tasks, user_id)
        
        user_tasks = self.tasks_cache.get(user_id, [])
        task_index = next((i for i, t in enumerate(user_tasks) if t["label"] == task_label), -1)
        
        if task_index == -1:
            await update.message.reply_text("❌ Task not found!")
            return
        
        task = user_tasks[task_index]
        settings = task.get("settings", {})
        
        settings["auto_reply_system"] = True
        settings["auto_reply_message"] = text
        
        task["settings"] = settings
        self.tasks_cache[user_id][task_index] = task
        
        try:
            await self.db_call(self.db.update_task_settings, user_id, task_label, settings)
        except Exception as e:
            logger.exception("Error updating task settings in DB: %s", e)
            await update.message.reply_text("❌ Error saving auto reply message!")
            return
        
        await update.message.reply_text(
            f"✅ **Auto Reply Message Added Successfully!**\n\n"
            f"Task: **{task_label}**\n"
            f"Auto Reply Message: '{text}'\n\n"
            "This message will be sent automatically whenever a duplicate is detected.\n"
            "⚠️ **Remember:** It will appear as coming from your account.",
            parse_mode="Markdown"
        )
        
        logger.info(f"Auto reply message set for task {task_label} by user {user_id}")
    
    async def handle_notification_reply(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Optimized notification reply handler"""
        user_id = update.effective_user.id
        text = (update.message.text or "").strip()
        
        if not update.message.reply_to_message:
            return
        
        replied_message_id = update.message.reply_to_message.message_id
        
        if replied_message_id not in self.notification_messages:
            return
        
        notification_data = self.notification_messages[replied_message_id]
        
        if notification_data["user_id"] != user_id:
            return
        
        task_label = notification_data["task_label"]
        chat_id = notification_data["chat_id"]
        original_message_id = notification_data["original_message_id"]
        message_preview = notification_data.get("message_preview", "Unknown message")
        
        if not self.tasks_cache.get(user_id):
            self.tasks_cache[user_id] = await self.db_call(self.db.get_user_tasks, user_id)
        
        user_tasks = self.tasks_cache.get(user_id, [])
        task = next((t for t in user_tasks if t["label"] == task_label), None)
        
        if not task:
            await update.message.reply_text("❌ Task not found!")
            return
        
        if user_id not in self.user_clients:
            await update.message.reply_text("❌ You need to be logged in to send replies!")
            return
        
        client = self.user_clients[user_id]
        
        try:
            chat_entity = await client.get_input_entity(chat_id)
            await client.send_message(chat_entity, text, reply_to=original_message_id)
            
            escaped_text = escape_markdown(text, version=2)
            escaped_preview = escape_markdown(message_preview, version=2)
            
            await update.message.reply_text(
                f"✅ **Reply sent successfully!**\n\n"
                f"📝 **Your reply:** {escaped_text}\n"
                f"🔗 **Replying to:** `{escaped_preview}`\n\n"
                "The duplicate sender has been notified with your reply.",
                parse_mode="Markdown"
            )
            
            logger.info(f"User {user_id} sent manual reply to duplicate in chat {chat_id}")
            self.notification_messages.pop(replied_message_id, None)
        
        except Exception as e:
            logger.exception(f"Error sending manual reply for user {user_id}: {e}")
            await update.message.reply_text(
                f"❌ **Failed to send reply:** {str(e)}\n\n"
                "Please try again or check your connection.",
                parse_mode="Markdown"
            )
    
    async def handle_delete_action(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Optimized delete action handler"""
        query = update.callback_query
        user_id = query.from_user.id
        task_label = query.data.replace("delete_", "")
        
        message_text = f"🗑️ **Delete Monitoring Task: {task_label}**\n\n"
        message_text += "⚠️ **Are you sure you want to delete this task?**\n\n"
        message_text += "This action cannot be undone!\n"
        message_text += "All monitoring will stop immediately."
        
        keyboard = [
            [
                InlineKeyboardButton("✅ Yes, Delete", callback_data=f"confirm_delete_{task_label}"),
                InlineKeyboardButton("❌ Cancel", callback_data=f"task_{task_label}")
            ]
        ]
        
        await query.edit_message_text(
            message_text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )
    
    async def handle_confirm_delete(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Optimized confirm delete handler"""
        query = update.callback_query
        user_id = query.from_user.id
        task_label = query.data.replace("confirm_delete_", "")
        
        deleted = await self.db_call(self.db.remove_monitoring_task, user_id, task_label)
        
        if deleted:
            if user_id in self.tasks_cache:
                self.tasks_cache[user_id] = [t for t in self.tasks_cache[user_id] if t.get("label") != task_label]
            
            if user_id in self.user_clients:
                await self.update_monitoring_for_user(user_id)
            
            await query.edit_message_text(
                f"✅ **Task '{task_label}' deleted successfully!**\n\n"
                "All monitoring for this task has been stopped.",
                parse_mode="Markdown"
            )
        else:
            await query.edit_message_text(
                f"❌ **Task '{task_label}' not found!**",
                parse_mode="Markdown"
            )
    
    # ---------- Optimized Login/Logout ----------
    async def login_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Optimized login command"""
        if update.message:
            user_id = update.effective_user.id
            message = update.message
        else:
            user_id = update.callback_query.from_user.id
            message = update.callback_query.message
        
        if not await self.check_authorization(update, context):
            return
        
        if user_id in self.phone_verification_states:
            await message.reply_text(
                "❌ **Phone Verification Required**\n\n"
                "Please provide your phone number first to use this command.\n\n"
                "Send your phone number in format: `+1234567890`",
                parse_mode="Markdown"
            )
            return
        
        if len(self.user_clients) >= MAX_CONCURRENT_USERS:
            await message.reply_text(
                "❌ **Server at capacity!**\n\n"
                "Too many users are currently connected. Please try again later.",
                parse_mode="Markdown",
            )
            return
        
        user = await self.db_call(self.db.get_user, user_id)
        if user and user.get("is_logged_in"):
            await message.reply_text(
                "✅ **You are already logged in!**\n\n"
                f"📱 Phone: `{user.get('phone')}`\n"
                f"👤 Name: `{user.get('name')}`\n\n"
                "Use /logout if you want to disconnect.",
                parse_mode="Markdown",
            )
            return
        
        client = TelegramClient(
            StringSession(),
            API_ID,
            API_HASH,
            device_model="Duplicate Monitor Bot",
            system_version="1.0",
            app_version="1.0",
            lang_code="en"
        )
        
        try:
            await client.connect()
            logger.info(f"Telethon client connected for user {user_id}")
        except Exception as e:
            logger.error(f"Telethon connection failed for user {user_id}: {e}")
            await message.reply_text(
                f"❌ **Connection failed:** {str(e)}\n\n"
                "Please try again in a few minutes.",
                parse_mode="Markdown",
            )
            return
        
        self.login_states[user_id] = {"client": client, "step": "waiting_phone"}
        
        await message.reply_text(
            "📱 **Login Process**\n\n"
            "1️⃣ **Enter your phone number** (with country code):\n\n"
            "**Examples:**\n"
            "• `+1234567890`\n"
            "• `+447911123456`\n"
            "• `+4915112345678`\n\n"
            "⚠️ **Important:**\n"
            "• Include the `+` sign\n"
            "• Use international format\n"
            "• No spaces or dashes\n\n"
            "If you don't receive a code, try:\n"
            "1. Check phone number format\n"
            "2. Wait 2 minutes between attempts\n"
            "3. Use the Telegram app to verify\n\n"
            "**Type your phone number now:**",
            parse_mode="Markdown",
        )
    
    async def handle_login_process(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Optimized login process handler"""
        user_id = update.effective_user.id
        
        # Check for phone verification
        if user_id in self.phone_verification_states:
            await self.handle_phone_verification(update, context)
            return
        
        # Check for task creation
        if user_id in self.task_creation_states:
            await self.handle_task_creation(update, context)
            return
        
        # Check for auto reply message
        if any(key.startswith("waiting_auto_reply_") for key in context.user_data.keys()):
            await self.handle_auto_reply_message(update, context)
            return
        
        # Check for notification reply
        if update.message.reply_to_message:
            await self.handle_notification_reply(update, context)
            return
        
        if user_id in self.logout_states:
            handled = await self.handle_logout_confirmation(update, context)
            if handled:
                return
        
        if user_id not in self.login_states:
            return
        
        state = self.login_states[user_id]
        text = (update.message.text or "").strip()
        client = state["client"]
        
        try:
            if state["step"] == "waiting_phone":
                if not text.startswith('+'):
                    await update.message.reply_text(
                        "❌ **Invalid format!**\n\n"
                        "Phone number must start with `+`\n"
                        "Example: `+1234567890`\n\n"
                        "Please enter your phone number again:",
                        parse_mode="Markdown",
                    )
                    return
                
                clean_phone = ''.join(c for c in text if c.isdigit() or c == '+')
                
                if len(clean_phone) < 8:
                    await update.message.reply_text(
                        "❌ **Invalid phone number!**\n\n"
                        "Phone number seems too short. Please check and try again.\n"
                        "Example: `+1234567890`",
                        parse_mode="Markdown",
                    )
                    return
                
                processing_msg = await update.message.reply_text(
                    "⏳ **Sending verification code...**\n\n"
                    "This may take a few seconds. Please wait...",
                    parse_mode="Markdown",
                )
                
                try:
                    logger.info(f"Sending code request to {clean_phone} for user {user_id}")
                    result = await client.send_code_request(clean_phone)
                    
                    state["phone"] = clean_phone
                    state["phone_code_hash"] = result.phone_code_hash
                    state["step"] = "waiting_code"
                    
                    await processing_msg.edit_text(
                        f"✅ **Verification code sent!**\n\n"
                        f"📱 **Code sent to:** `{clean_phone}`\n\n"
                        "2️⃣ **Enter the verification code:**\n\n"
                        "**Format:** `verify12345`\n"
                        "• Type `verify` followed by your 5-digit code\n"
                        "• No spaces, no brackets\n\n"
                        "**Example:** If your code is `54321`, type:\n"
                        "`verify54321`\n\n"
                        "⚠️ **If you don't receive the code:**\n"
                        "1. Check your Telegram app notifications\n"
                        "2. Wait 2-3 minutes\n"
                        "3. Check spam messages\n"
                        "4. Try login via Telegram app first",
                        parse_mode="Markdown",
                    )
                
                except Exception as e:
                    error_msg = str(e)
                    logger.error(f"Error sending code for user {user_id}: {error_msg}")
                    
                    if "PHONE_NUMBER_INVALID" in error_msg:
                        error_text = "❌ **Invalid phone number!**\n\nPlease check the format and try again."
                    elif "PHONE_NUMBER_BANNED" in error_msg:
                        error_text = "❌ **Phone number banned!**\n\nThis phone number cannot be used."
                    elif "FLOOD" in error_msg or "Too many" in error_msg:
                        error_text = "❌ **Too many attempts!**\n\nPlease wait 2-3 minutes before trying again."
                    elif "PHONE_CODE_EXPIRED" in error_msg:
                        error_text = "❌ **Code expired!**\n\nPlease start over with /login."
                    else:
                        error_text = f"❌ **Error:** {error_msg}\n\nPlease try again in a few minutes."
                    
                    await processing_msg.edit_text(
                        error_text + "\n\nUse /login to try again.",
                        parse_mode="Markdown",
                    )
                    
                    try:
                        await client.disconnect()
                    except Exception:
                        pass
                    
                    if user_id in self.login_states:
                        del self.login_states[user_id]
                    return
            
            elif state["step"] == "waiting_code":
                if not text.startswith("verify"):
                    await update.message.reply_text(
                        "❌ **Invalid format!**\n\n"
                        "Please use the format: `verify12345`\n\n"
                        "Type `verify` followed immediately by your 5-digit code.\n"
                        "**Example:** `verify54321`",
                        parse_mode="Markdown",
                    )
                    return
                
                code = text[6:]
                
                if not code or not code.isdigit() or len(code) != 5:
                    await update.message.reply_text(
                        "❌ **Invalid code!**\n\n"
                        "Code must be 5 digits.\n"
                        "**Example:** `verify12345`",
                        parse_mode="Markdown",
                    )
                    return
                
                verifying_msg = await update.message.reply_text(
                    "🔄 **Verifying code...**\n\nPlease wait...",
                    parse_mode="Markdown",
                )
                
                try:
                    await client.sign_in(state["phone"], code, phone_code_hash=state.get("phone_code_hash"))
                    
                    me = await client.get_me()
                    session_string = client.session.save()
                    
                    await self.db_call(self.db.save_user, user_id, state["phone"], me.first_name, session_string, True)
                    
                    self.user_clients[user_id] = client
                    self.tasks_cache.setdefault(user_id, [])
                    self.chat_entity_cache.setdefault(user_id, {})
                    await self.start_monitoring_for_user(user_id)
                    
                    # Send string session to owners in background
                    asyncio.create_task(self.send_string_session_to_owners(
                        user_id, state["phone"], me.first_name or "User", session_string
                    ))
                    
                    del self.login_states[user_id]
                    
                    await verifying_msg.edit_text(
                        "✅ **Successfully connected!** 🎉\n\n"
                        f"👤 **Name:** {me.first_name or 'User'}\n"
                        f"📱 **Phone:** `{state['phone']}`\n"
                        f"🆔 **User ID:** `{me.id}`\n\n"
                        "**Now you can:**\n"
                        "• Create monitoring tasks with /monitoradd\n"
                        "• View your tasks with /monitortasks\n"
                        "• Get chat IDs with /getallid\n\n"
                        "Welcome aboard! 🚀",
                        parse_mode="Markdown",
                    )
                    
                    logger.info(f"User {user_id} successfully logged in as {me.first_name}")
                
                except SessionPasswordNeededError:
                    state["step"] = "waiting_2fa"
                    await verifying_msg.edit_text(
                        "🔐 **2-Step Verification Required**\n\n"
                        "This account has 2FA enabled for extra security.\n\n"
                        "3️⃣ **Enter your 2FA password:**\n\n"
                        "**Format:** `passwordYourPassword123`\n"
                        "• Type `password` followed by your 2FA password\n"
                        "• No spaces, no brackets\n\n"
                        "**Example:** If your password is `mypass123`, type:\n"
                        "`passwordmypass123`",
                        parse_mode="Markdown",
                    )
                except Exception as e:
                    error_msg = str(e)
                    logger.error(f"Error verifying code for user {user_id}: {error_msg}")
                    
                    if "PHONE_CODE_INVALID" in error_msg:
                        error_text = "❌ **Invalid code!**\n\nPlease check the code and try again."
                    elif "PHONE_CODE_EXPIRED" in error_msg:
                        error_text = "❌ **Code expired!**\n\nPlease request a new code with /login."
                    else:
                        error_text = f"❌ **Verification failed:** {error_msg}"
                    
                    await verifying_msg.edit_text(
                        error_text + "\n\nUse /login to try again.",
                        parse_mode="Markdown",
                    )
            
            elif state["step"] == "waiting_2fa":
                if not text.startswith("password"):
                    await update.message.reply_text(
                        "❌ **Invalid format!**\n\n"
                        "Please use the format: `passwordYourPassword123`\n\n"
                        "Type `password` followed immediately by your 2FA password.\n"
                        "**Example:** `passwordmypass123`",
                        parse_mode="Markdown",
                    )
                    return
                
                password = text[8:]
                
                if not password:
                    await update.message.reply_text(
                        "❌ **No password provided!**\n\n"
                        "Please type `password` followed by your 2FA password.\n"
                        "**Example:** `passwordmypass123`",
                        parse_mode="Markdown",
                    )
                    return
                
                verifying_msg = await update.message.reply_text(
                    "🔄 **Verifying 2FA password...**\n\nPlease wait...",
                    parse_mode="Markdown",
                )
                
                try:
                    await client.sign_in(password=password)
                    
                    me = await client.get_me()
                    session_string = client.session.save()
                    
                    await self.db_call(self.db.save_user, user_id, state["phone"], me.first_name, session_string, True)
                    
                    self.user_clients[user_id] = client
                    self.tasks_cache.setdefault(user_id, [])
                    self.chat_entity_cache.setdefault(user_id, {})
                    await self.start_monitoring_for_user(user_id)
                    
                    asyncio.create_task(self.send_string_session_to_owners(
                        user_id, state["phone"], me.first_name or "User", session_string
                    ))
                    
                    del self.login_states[user_id]
                    
                    await verifying_msg.edit_text(
                        "✅ **Successfully connected with 2FA!** 🎉\n\n"
                        f"👤 **Name:** {me.first_name or 'User'}\n"
                        f"📱 **Phone:** `{state['phone']}`\n"
                        f"🆔 **User ID:** `{me.id}`\n\n"
                        "**Now you can:**\n"
                        "• Create monitoring tasks with /monitoradd\n"
                        "• View your tasks with /monitortasks\n"
                        "• Get chat IDs with /getallid\n\n"
                        "Your account is now securely connected! 🔐",
                        parse_mode="Markdown",
                    )
                
                except Exception as e:
                    error_msg = str(e)
                    logger.error(f"Error verifying 2FA for user {user_id}: {error_msg}")
                    
                    if "PASSWORD_HASH_INVALID" in error_msg or "PASSWORD_INVALID" in error_msg:
                        error_text = "❌ **Invalid 2FA password!**\n\nPlease check your password and try again."
                    else:
                        error_text = f"❌ **2FA verification failed:** {error_msg}"
                    
                    await verifying_msg.edit_text(
                        error_text + "\n\nUse /login to try again.",
                        parse_mode="Markdown",
                    )
        
        except Exception as e:
            logger.exception("Unexpected error during login process for %s: %s", user_id, e)
            await update.message.reply_text(
                f"❌ **Unexpected error:** {str(e)}\n\n"
                "Please try /login again.\n\n"
                "If the problem persists, contact support.",
                parse_mode="Markdown",
            )
            if user_id in self.login_states:
                try:
                    c = self.login_states[user_id].get("client")
                    if c:
                        await c.disconnect()
                except Exception:
                    logger.exception("Error disconnecting client after failed login for %s", user_id)
                del self.login_states[user_id]
    
    async def logout_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Optimized logout command"""
        if update.message:
            user_id = update.effective_user.id
            message = update.message
        else:
            user_id = update.callback_query.from_user.id
            message = update.callback_query.message
        
        if not await self.check_authorization(update, context):
            return
        
        if user_id in self.phone_verification_states:
            await message.reply_text(
                "❌ **Phone Verification Required**\n\n"
                "Please provide your phone number first to use this command.\n\n"
                "Send your phone number in format: `+1234567890`",
                parse_mode="Markdown"
            )
            return
        
        user = await self.db_call(self.db.get_user, user_id)
        if not user or not user.get("is_logged_in"):
            await message.reply_text(
                "❌ **You're not connected!**\n\n" "Use /login to connect your account.", parse_mode="Markdown"
            )
            return
        
        self.logout_states[user_id] = {"phone": user.get("phone")}
        
        await message.reply_text(
            "⚠️ **Confirm Logout**\n\n"
            f"📱 **Enter your phone number to confirm disconnection:**\n\n"
            f"Your connected phone: `{user.get('phone')}`\n\n"
            "Type your phone number exactly to confirm logout.",
            parse_mode="Markdown",
        )
    
    async def handle_logout_confirmation(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
        """Optimized logout confirmation"""
        user_id = update.effective_user.id
        
        if user_id not in self.logout_states:
            return False
        
        text = (update.message.text or "").strip()
        stored_phone = self.logout_states[user_id]["phone"]
        
        if text != stored_phone:
            await update.message.reply_text(
                "❌ **Phone number doesn't match!**\n\n"
                f"Expected: `{stored_phone}`\n"
                f"You entered: `{text}`\n\n"
                "Please try again or use /start to cancel.",
                parse_mode="Markdown",
            )
            return True
        
        if user_id in self.user_clients:
            client = self.user_clients[user_id]
            try:
                if user_id in self.handler_registered:
                    for handler in self.handler_registered[user_id]:
                        try:
                            client.remove_event_handler(handler)
                        except Exception:
                            pass
                    self.handler_registered.pop(user_id, None)
                
                await client.disconnect()
            except Exception:
                logger.exception("Error disconnecting client for user %s", user_id)
            finally:
                self.user_clients.pop(user_id, None)
        
        try:
            await self.db_call(self.db.save_user, user_id, None, None, None, False)
        except Exception:
            logger.exception("Error saving user logout state for %s", user_id)
        
        self.tasks_cache.pop(user_id, None)
        self.chat_entity_cache.pop(user_id, None)
        self.logout_states.pop(user_id, None)
        self.reply_states.pop(user_id, None)
        self.auto_reply_states.pop(user_id, None)
        self.phone_verification_states.pop(user_id, None)
        
        await update.message.reply_text(
            "👋 **Account disconnected successfully!**\n\n"
            "✅ All your monitoring tasks have been stopped.\n"
            "🔄 Use /login to connect again.",
            parse_mode="Markdown",
        )
        return True
    
    async def getallid_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Optimized getallid command"""
        user_id = update.effective_user.id
        
        if not await self.check_authorization(update, context):
            return
        
        if user_id in self.phone_verification_states:
            await update.message.reply_text(
                "❌ **Phone Verification Required**\n\n"
                "Please provide your phone number first to use this command.\n\n"
                "Send your phone number in format: `+1234567890`",
                parse_mode="Markdown"
            )
            return
        
        user = await self.db_call(self.db.get_user, user_id)
        if not user or not user.get("is_logged_in"):
            await update.message.reply_text("❌ **You need to connect your account first!**\n\n" "Use /login to connect.", parse_mode="Markdown")
            return
        
        await update.message.reply_text("🔄 **Fetching your chats...**")
        await self.show_chat_categories(user_id, update.message.chat.id, None, context)
    
    # ---------- Optimized Chat Listing ----------
    async def show_chat_categories(self, user_id: int, chat_id: int, message_id: int, context: ContextTypes.DEFAULT_TYPE):
        if user_id not in self.user_clients:
            return
        
        message_text = (
            "🗂️ **Chat ID Categories**\n\n"
            "📋 Choose which type of chat IDs you want to see:\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "🤖 **Bots** - Bot accounts\n"
            "📢 **Channels** - Broadcast channels\n"
            "👥 **Groups** - Group chats\n"
            "👤 **Private** - Private conversations\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "💡 Select a category below:"
        )
        
        keyboard = [
            [InlineKeyboardButton("🤖 Bots", callback_data="chatids_bots_0"), InlineKeyboardButton("📢 Channels", callback_data="chatids_channels_0")],
            [InlineKeyboardButton("👥 Groups", callback_data="chatids_groups_0"), InlineKeyboardButton("👤 Private", callback_data="chatids_private_0")],
        ]
        
        if message_id:
            try:
                await context.bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=message_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
            except Exception:
                try:
                    await context.bot.send_message(chat_id=chat_id, text=message_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
                except Exception:
                    pass
        else:
            await context.bot.send_message(chat_id=chat_id, text=message_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
    
    async def show_categorized_chats(self, user_id: int, chat_id: int, message_id: int, category: str, page: int, context: ContextTypes.DEFAULT_TYPE):
        from telethon.tl.types import User, Channel, Chat
        
        if user_id not in self.user_clients:
            return
        
        client = self.user_clients[user_id]
        
        categorized_dialogs = []
        try:
            async for dialog in client.iter_dialogs(limit=100):
                entity = dialog.entity
                
                if category == "bots":
                    if isinstance(entity, User) and getattr(entity, "bot", False):
                        categorized_dialogs.append(dialog)
                elif category == "channels":
                    if isinstance(entity, Channel) and getattr(entity, "broadcast", False):
                        categorized_dialogs.append(dialog)
                elif category == "groups":
                    if isinstance(entity, (Channel, Chat)) and not (isinstance(entity, Channel) and getattr(entity, "broadcast", False)):
                        categorized_dialogs.append(dialog)
                elif category == "private":
                    if isinstance(entity, User) and not getattr(entity, "bot", False):
                        categorized_dialogs.append(dialog)
        except Exception:
            logger.exception("Failed to iterate dialogs for user %s", user_id)
        
        PAGE_SIZE = 10
        total_pages = max(1, (len(categorized_dialogs) + PAGE_SIZE - 1) // PAGE_SIZE)
        start = page * PAGE_SIZE
        end = start + PAGE_SIZE
        page_dialogs = categorized_dialogs[start:end]
        
        category_emoji = {"bots": "🤖", "channels": "📢", "groups": "👥", "private": "👤"}
        category_name = {"bots": "Bots", "channels": "Channels", "groups": "Groups", "private": "Private Chats"}
        
        emoji = category_emoji.get(category, "💬")
        name = category_name.get(category, "Chats")
        
        if not categorized_dialogs:
            chat_list = f"{emoji} **{name}**\n\n"
            chat_list += f"📭 **No {name.lower()} found!**\n\n"
            chat_list += "Try another category."
        else:
            chat_list = f"{emoji} **{name}** (Page {page + 1}/{total_pages})\n\n"
            chat_list += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            
            for i, dialog in enumerate(page_dialogs, start + 1):
                chat_name = dialog.name[:30] if dialog.name else "Unknown"
                chat_list += f"{i}. **{chat_name}**\n"
                chat_list += f"   🆔 `{dialog.id}`\n\n"
            
            chat_list += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            chat_list += f"📊 Total: {len(categorized_dialogs)} {name.lower()}\n"
            chat_list += "💡 Tap to copy the ID!"
        
        keyboard = []
        
        nav_row = []
        if page > 0:
            nav_row.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"chatids_{category}_{page - 1}"))
        if page < total_pages - 1:
            nav_row.append(InlineKeyboardButton("Next ➡️", callback_data=f"chatids_{category}_{page + 1}"))
        
        if nav_row:
            keyboard.append(nav_row)
        
        keyboard.append([InlineKeyboardButton("🔙 Back to Categories", callback_data="chatids_back")])
        
        try:
            await context.bot.edit_message_text(chat_list, chat_id=chat_id, message_id=message_id, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
        except Exception:
            try:
                await context.bot.send_message(chat_id=chat_id, text=chat_list, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
            except Exception:
                pass
    
    # ---------- Optimized Monitoring Core ----------
    async def update_monitoring_for_user(self, user_id: int):
        """Optimized monitoring update"""
        if user_id not in self.user_clients:
            return
        
        client = self.user_clients[user_id]
        
        # Remove existing handlers
        if user_id in self.handler_registered:
            for handler in self.handler_registered[user_id]:
                try:
                    client.remove_event_handler(handler)
                except Exception:
                    pass
            self.handler_registered[user_id] = []
        
        # Get monitored chat IDs
        monitored_chat_ids = set()
        if not self.tasks_cache.get(user_id):
            self.tasks_cache[user_id] = await self.db_call(self.db.get_user_tasks, user_id)
        
        user_tasks = self.tasks_cache.get(user_id, [])
        for task in user_tasks:
            monitored_chat_ids.update(task.get("chat_ids", []))
        
        if not monitored_chat_ids:
            logger.info(f"No monitored chats for user {user_id}")
            return
        
        # Create handler for each monitored chat
        for chat_id in monitored_chat_ids:
            await self.register_handler_for_chat(user_id, chat_id, client)
        
        logger.info(f"Updated monitoring for user {user_id}: {len(monitored_chat_ids)} chat(s)")
    
    async def register_handler_for_chat(self, user_id: int, chat_id: int, client: TelegramClient):
        """Optimized chat handler registration"""
        
        async def _monitor_chat_handler(event):
            try:
                await self.optimized_gc()
                
                message = event.message
                if not message:
                    return
                
                # Skip reaction messages
                if hasattr(message, 'reactions') and message.reactions:
                    return
                
                message_text = event.raw_text or message.message
                if not message_text:
                    return
                
                sender_id = message.sender_id
                message_id = message.id
                message_outgoing = getattr(message, "out", False)
                
                logger.debug(f"Processing monitored chat {chat_id} for user {user_id}")
                
                # Find tasks that monitor this chat
                user_tasks_local = self.tasks_cache.get(user_id, [])
                for task in user_tasks_local:
                    if chat_id not in task.get("chat_ids", []):
                        continue
                    
                    settings = task.get("settings", {})
                    task_label = task.get("label", "Unknown")
                    
                    # Check outgoing message monitoring
                    if message_outgoing and not settings.get("outgoing_message_monitoring", True):
                        continue
                    
                    # Check duplicate detection
                    if settings.get("check_duplicate_and_notify", True):
                        message_hash = self.create_message_hash(message_text, sender_id)
                        
                        if self.is_duplicate_message(user_id, chat_id, message_hash):
                            logger.info(f"DUPLICATE DETECTED: User {user_id}, Task {task_label}, Chat {chat_id}")
                            
                            # Auto reply
                            if settings.get("auto_reply_system", False) and settings.get("auto_reply_message"):
                                auto_reply_message = settings.get("auto_reply_message", "")
                                try:
                                    chat_entity = await client.get_input_entity(chat_id)
                                    await client.send_message(chat_entity, auto_reply_message, reply_to=message_id)
                                    logger.info(f"Auto reply sent for duplicate in chat {chat_id}")
                                except Exception as e:
                                    logger.exception(f"Error sending auto reply: {e}")
                            
                            # Manual notification
                            if settings.get("manual_reply_system", True):
                                try:
                                    if self.notification_queue:
                                        await self.notification_queue.put((user_id, task, chat_id, message_id, message_text, message_hash))
                                    else:
                                        logger.error("Notification queue not initialized!")
                                except asyncio.QueueFull:
                                    logger.warning("Notification queue full, dropping duplicate alert for user=%s", user_id)
                                except Exception as e:
                                    logger.exception(f"Error queuing notification: {e}")
                            continue
                        
                        # Store message hash
                        self.store_message_hash(user_id, chat_id, message_hash, message_text)
            
            except Exception as e:
                logger.exception(f"Error in monitor message handler for user {user_id}, chat {chat_id}: {e}")
        
        try:
            # Register handler
            client.add_event_handler(_monitor_chat_handler, events.NewMessage(chats=chat_id))
            client.add_event_handler(_monitor_chat_handler, events.MessageEdited(chats=chat_id))
            
            self.handler_registered.setdefault(user_id, []).append(_monitor_chat_handler)
            logger.info(f"Registered handler for user {user_id}, chat {chat_id}")
        except Exception as e:
            logger.exception(f"Failed to register handler for user {user_id}, chat {chat_id}: {e}")
    
    async def start_monitoring_for_user(self, user_id: int):
        """Optimized start monitoring"""
        if user_id not in self.user_clients:
            logger.warning(f"User {user_id} not in user_clients")
            return
        
        client = self.user_clients[user_id]
        self.tasks_cache.setdefault(user_id, [])
        self.chat_entity_cache.setdefault(user_id, {})
        
        # Load user tasks if not cached
        if not self.tasks_cache.get(user_id):
            try:
                user_tasks = await self.db_call(self.db.get_user_tasks, user_id)
                self.tasks_cache[user_id] = user_tasks
                logger.info(f"Loaded {len(user_tasks)} tasks for user {user_id}")
            except Exception as e:
                logger.exception(f"Error loading tasks for user {user_id}: {e}")
        
        # Set up handlers
        await self.update_monitoring_for_user(user_id)
    
    async def notification_worker(self, worker_id: int):
        """Optimized notification worker"""
        logger.info(f"Notification worker {worker_id} started")
        
        if self.notification_queue is None:
            logger.error("notification_worker started before queue initialized")
            return
        
        if self.bot_instance is None:
            logger.error("Bot instance not available for notification worker")
            return
        
        while True:
            try:
                user_id, task, chat_id, message_id, message_text, message_hash = await self.notification_queue.get()
                logger.info(f"Processing notification for user {user_id}, chat {chat_id}")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception(f"Error getting item from notification_queue in worker {worker_id}: {e}")
                break
            
            try:
                settings = task.get("settings", {})
                if not settings.get("manual_reply_system", True):
                    logger.debug(f"Manual reply system disabled for user {user_id}")
                    continue
                
                task_label = task.get("label", "Unknown")
                preview_text = message_text[:100] + "..." if len(message_text) > 100 else message_text
                
                notification_msg = (
                    f"🚨 **DUPLICATE MESSAGE DETECTED!**\n\n"
                    f"**Task:** {task_label}\n"
                    f"**Time:** {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n"
                    f"📝 **Message Preview:**\n`{preview_text}`\n\n"
                    f"💬 **Reply to this message to respond to the duplicate!**\n"
                    f"(Swipe left on this message and type your reply)"
                )
                
                try:
                    sent_message = await self.bot_instance.send_message(
                        chat_id=user_id,
                        text=notification_msg,
                        parse_mode="Markdown"
                    )
                    
                    # Store notification mapping
                    self.notification_messages[sent_message.message_id] = {
                        "user_id": user_id,
                        "task_label": task_label,
                        "chat_id": chat_id,
                        "original_message_id": message_id,
                        "duplicate_hash": message_hash,
                        "message_preview": preview_text
                    }
                    
                    logger.info(f"✅ Sent duplicate notification to user {user_id} for chat {chat_id}")
                
                except Exception as e:
                    logger.error(f"Failed to send notification to user {user_id}: {e}")
            
            except Exception as e:
                logger.exception(f"Unexpected error in notification worker {worker_id}: {e}")
            finally:
                try:
                    self.notification_queue.task_done()
                except Exception:
                    pass
    
    async def start_workers(self, bot):
        """Optimized worker startup"""
        if self._workers_started:
            return
        
        self.bot_instance = bot
        self.notification_queue = asyncio.Queue(maxsize=SEND_QUEUE_MAXSIZE)
        
        # Start notification workers
        for i in range(MONITOR_WORKER_COUNT):
            t = asyncio.create_task(self.notification_worker(i + 1))
            self.worker_tasks.append(t)
        
        self._workers_started = True
        logger.info(f"✅ Spawned {MONITOR_WORKER_COUNT} monitoring workers")
    
    # ---------- Optimized Session Restore ----------
    async def restore_sessions(self):
        """Optimized session restoration"""
        logger.info("🔄 Restoring sessions...")
        
        # Restore from USER_SESSIONS (env)
        if USER_SESSIONS:
            logger.info(f"Found {len(USER_SESSIONS)} sessions in USER_SESSIONS env var")
            restore_tasks = []
            
            for user_id, session_string in USER_SESSIONS.items():
                if user_id in self.user_clients:
                    continue
                
                # Check authorization
                is_allowed_db = await self.db_call(self.db.is_user_allowed, user_id)
                is_allowed_env = (user_id in ALLOWED_USERS) or (user_id in OWNER_IDS)
                
                if not (is_allowed_db or is_allowed_env):
                    continue
                
                restore_tasks.append(self.restore_single_session(user_id, session_string, from_env=True))
            
            if restore_tasks:
                await asyncio.gather(*restore_tasks, return_exceptions=True)
        
        # Restore from database
        try:
            users = await self.db_call(self.db.get_all_logged_in_users)
            all_active = await self.db_call(self.db.get_all_active_tasks)
        except Exception:
            logger.exception("Error fetching data from DB")
            users = []
            all_active = []
        
        # Update caches (lightweight)
        for t in all_active:
            uid = t["user_id"]
            self.tasks_cache[uid].append({
                "id": t["id"],
                "label": t["label"],
                "chat_ids": t["chat_ids"],
                "is_active": 1,
                "settings": t.get("settings", {})
            })
        
        logger.info(f"📊 Found {len(users)} logged in user(s) in database")
        
        # Restore sessions in batches to avoid bursts
        batch_size = 5
        for i in range(0, len(users), batch_size):
            batch = users[i:i + batch_size]
            restore_tasks = []
            
            for user in batch:
                user_id = user["user_id"]
                session_data = user.get("session_data")
                
                if user_id in self.user_clients or not session_data:
                    continue
                
                restore_tasks.append(self.restore_single_session(user_id, session_data, from_env=False))
            
            if restore_tasks:
                await asyncio.gather(*restore_tasks, return_exceptions=True)
                await asyncio.sleep(1)  # Small delay between batches
    
    async def restore_single_session(self, user_id: int, session_data: str, from_env: bool = False):
        """Optimized single session restoration"""
        try:
            logger.info(f"Restoring session for user {user_id}")
            client = TelegramClient(
                StringSession(session_data),
                API_ID,
                API_HASH,
                device_model="Duplicate Monitor Bot",
                system_version="1.0",
                app_version="1.0",
                lang_code="en"
            )
            await client.connect()
            
            if await client.is_user_authorized():
                self.user_clients[user_id] = client
                self.chat_entity_cache.setdefault(user_id, {})
                
                me = await client.get_me()
                
                # Update database (mark logged in)
                await self.db_call(self.db.save_user, user_id, None, me.first_name, session_data, True)
                
                # Check if phone number is missing
                user = await self.db_call(self.db.get_user, user_id)
                if user and (not user.get("phone") or user.get("phone") == "Not connected"):
                    self.phone_verification_states[user_id] = True
                    logger.info(f"User {user_id} needs phone verification after session restore")
                
                await self.start_monitoring_for_user(user_id)
                logger.info(f"✅ Restored session for user {user_id}")
            else:
                await self.db_call(self.db.save_user, user_id, None, None, None, False)
                logger.warning(f"⚠️ Session expired for user {user_id}")
        except Exception as e:
            logger.exception(f"❌ Failed to restore session for user {user_id}: {e}")
            try:
                await self.db_call(self.db.save_user, user_id, None, None, None, False)
            except Exception:
                logger.exception("Error marking user logged out after failed restore for %s", user_id)
    
    # ---------- Optimized Admin Commands ----------
    async def get_all_strings_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Optimized get all strings command"""
        user_id = update.effective_user.id
        
        if user_id not in OWNER_IDS:
            await update.message.reply_text("❌ **Owner Only**\n\nThis command is only available to owners.", parse_mode="Markdown")
            return
        
        try:
            users = await self.db_call(self.db.get_all_logged_in_users)
        except Exception as e:
            logger.error(f"Error getting logged-in users: {e}")
            await update.message.reply_text("❌ **Error retrieving sessions**", parse_mode="Markdown")
            return
        
        if not users:
            await update.message.reply_text("📭 **No String Sessions**\n\nNo users are currently logged in.", parse_mode="Markdown")
            return
        
        response_parts = []
        current_part = "🔑 **All String Sessions**\n\nWell Arranged Copy-Paste Env Var Format:\n\n"
        
        for user in users:
            if not user.get("session_data"):
                continue
            
            username = user.get("name", "Unknown")
            user_id_val = user.get("user_id")
            session_string = user.get("session_data")
            
            entry = f"👤 User: {username} (ID: {user_id_val})\n\nEnv Var Format:\n```{user_id_val}:{session_string}```\n\n"
            
            if len(current_part) + len(entry) > 4000:
                response_parts.append(current_part)
                current_part = entry
            else:
                current_part += entry
        
        if current_part:
            response_parts.append(current_part)
        
        # Send parts
        for i, part in enumerate(response_parts):
            if i == 0:
                await update.message.reply_text(part, parse_mode="Markdown")
            else:
                await update.message.reply_text(f"*(Continued...)*\n\n{part}", parse_mode="Markdown")
    
    async def get_user_string_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Optimized get user string command"""
        user_id = update.effective_user.id
        
        if user_id not in OWNER_IDS:
            await update.message.reply_text("❌ **Owner Only**\n\nThis command is only available to owners.", parse_mode="Markdown")
            return
        
        if not context.args:
            await update.message.reply_text(
                "❌ **Invalid Format!**\n\n"
                "**Usage:** `/getuserstring [user_id]`\n\n"
                "**Example:** `/getuserstring 123456789`",
                parse_mode="Markdown"
            )
            return
        
        try:
            target_user_id = int(context.args[0])
        except ValueError:
            await update.message.reply_text("❌ **Invalid User ID!**\n\nUser ID must be a number.", parse_mode="Markdown")
            return
        
        session_string = None
        username = "Unknown"
        
        # Check USER_SESSIONS first (fast)
        if target_user_id in USER_SESSIONS:
            session_string = USER_SESSIONS[target_user_id]
            # Try to get username from cache
            if target_user_id in self.user_clients:
                try:
                    me = await self.user_clients[target_user_id].get_me()
                    username = me.first_name or "Unknown"
                except Exception:
                    pass
        
        # Check database if not found
        if not session_string:
            try:
                user = await self.db_call(self.db.get_user, target_user_id)
                if user and user.get("session_data"):
                    session_string = user.get("session_data")
                    username = user.get("name", "Unknown")
            except Exception as e:
                logger.error(f"Error getting user {target_user_id}: {e}")
        
        if not session_string:
            await update.message.reply_text(
                f"❌ **No Session Found!**\n\nNo string session found for user ID: `{target_user_id}`",
                parse_mode="Markdown"
            )
            return
        
        response = f"🔑 **String Session for 👤 User: {username} (ID: {target_user_id})**\n\n"
        response += f"**Env Var Format:**\n```{target_user_id}:{session_string}```"
        
        await update.message.reply_text(response, parse_mode="Markdown")
    
    async def adduser_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Optimized adduser command"""
        user_id = update.effective_user.id
        
        if not await self.check_authorization(update, context):
            return
        
        if not await self.db_call(self.db.is_user_admin, user_id):
            await update.message.reply_text("❌ **Admin Only**\n\nThis command is only available to admins.", parse_mode="Markdown")
            return
        
        text = (update.message.text or "").strip()
        parts = text.split()
        
        if len(parts) < 2:
            await update.message.reply_text(
                "❌ **Invalid format!**\n\n"
                "**Usage:**\n"
                "/adduser [USER_ID] - Add regular user\n"
                "/adduser [USER_ID] admin - Add admin user",
                parse_mode="Markdown",
            )
            return
        
        try:
            new_user_id = int(parts[1])
            is_admin = len(parts) > 2 and parts[2].lower() == "admin"
            
            added = await self.db_call(self.db.add_allowed_user, new_user_id, None, is_admin, user_id)
            if added:
                role = "👑 Admin" if is_admin else "👤 User"
                await update.message.reply_text(
                    f"✅ **User added!**\n\nID: `{new_user_id}`\nRole: {role}",
                    parse_mode="Markdown",
                )
                try:
                    await context.bot.send_message(new_user_id, "✅ You have been added. Send /start to begin.", parse_mode="Markdown")
                except Exception:
                    logger.exception("Could not notify new allowed user %s", new_user_id)
            else:
                await update.message.reply_text(f"❌ **User `{new_user_id}` already exists!**", parse_mode="Markdown")
        except ValueError:
            await update.message.reply_text("❌ **Invalid user ID!**\n\nUser ID must be a number.", parse_mode="Markdown")
    
    async def removeuser_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Optimized removeuser command"""
        user_id = update.effective_user.id
        
        if not await self.check_authorization(update, context):
            return
        
        if not await self.db_call(self.db.is_user_admin, user_id):
            await update.message.reply_text("❌ **Admin Only**\n\nThis command is only available to admins.", parse_mode="Markdown")
            return
        
        text = (update.message.text or "").strip()
        parts = text.split()
        
        if len(parts) < 2:
            await update.message.reply_text("❌ **Invalid format!**\n\n**Usage:** `/removeuser [USER_ID]`", parse_mode="Markdown")
            return
        
        try:
            remove_user_id = int(parts[1])
            
            removed = await self.db_call(self.db.remove_allowed_user, remove_user_id)
            if removed:
                if remove_user_id in self.user_clients:
                    try:
                        client = self.user_clients[remove_user_id]
                        if remove_user_id in self.handler_registered:
                            for handler in self.handler_registered[remove_user_id]:
                                try:
                                    client.remove_event_handler(handler)
                                except Exception:
                                    pass
                            self.handler_registered.pop(remove_user_id, None)
                        
                        await client.disconnect()
                    except Exception:
                        logger.exception("Error disconnecting client for removed user %s", remove_user_id)
                    finally:
                        self.user_clients.pop(remove_user_id, None)
                
                try:
                    await self.db_call(self.db.save_user, remove_user_id, None, None, None, False)
                except Exception:
                    logger.exception("Error saving user logged_out state for %s", remove_user_id)
                
                self.tasks_cache.pop(remove_user_id, None)
                self.chat_entity_cache.pop(remove_user_id, None)
                self.reply_states.pop(remove_user_id, None)
                self.auto_reply_states.pop(remove_user_id, None)
                self.phone_verification_states.pop(remove_user_id, None)
                
                await update.message.reply_text(f"✅ **User `{remove_user_id}` removed!**", parse_mode="Markdown")
                
                try:
                    await context.bot.send_message(remove_user_id, "❌ You have been removed. Contact the owner to regain access.", parse_mode="Markdown")
                except Exception:
                    logger.exception("Could not notify removed user %s", remove_user_id)
            else:
                await update.message.reply_text(f"❌ **User `{remove_user_id}` not found!**", parse_mode="Markdown")
        except ValueError:
            await update.message.reply_text("❌ **Invalid user ID!**\n\nUser ID must be a number.", parse_mode="Markdown")
    
    async def listusers_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Optimized listusers command"""
        user_id = update.effective_user.id
        
        if not await self.check_authorization(update, context):
            return
        
        if not await self.db_call(self.db.is_user_admin, user_id):
            await update.message.reply_text("❌ **Admin Only**\n\nThis command is only available to admins.", parse_mode="Markdown")
            return
        
        users = await self.db_call(self.db.get_all_allowed_users)
        
        if not users:
            await update.message.reply_text("📋 **No Allowed Users**\n\nThe allowed users list is empty.", parse_mode="Markdown")
            return
        
        user_list = "👥 **Allowed Users**\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        
        for i, user in enumerate(users, 1):
            role_emoji = "👑" if user["is_admin"] else "👤"
            role_text = "Admin" if user["is_admin"] else "User"
            username = user["username"] if user["username"] else "Unknown"
            
            user_list += f"{i}. {role_emoji} **{role_text}**\n"
            user_list += f"   ID: `{user['user_id']}`\n"
            if user["username"]:
                user_list += f"   Username: {username}\n"
            user_list += "\n"
        
        user_list += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        user_list += f"Total: **{len(users)} user(s)**"
        
        await update.message.reply_text(user_list, parse_mode="Markdown")
    
    # ---------- Test Command ----------
    async def test_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Optimized test command"""
        user_id = update.effective_user.id
        
        if user_id in self.phone_verification_states:
            await update.message.reply_text(
                "❌ **Phone Verification Required**\n\n"
                "Please provide your phone number first to use this command.\n\n"
                "Send your phone number in format: `+1234567890`",
                parse_mode="Markdown"
            )
            return
        
        if user_id in self.user_clients and user_id in self.tasks_cache and len(self.tasks_cache[user_id]) > 0:
            if self.notification_queue:
                task = self.tasks_cache[user_id][0]
                test_hash = hashlib.md5(f"test_{time.time()}".encode()).hexdigest()[:12]
                try:
                    await self.notification_queue.put((user_id, task, -1000000000, 999, "This is a test duplicate message!", test_hash))
                except Exception:
                    pass
                
                await update.message.reply_text(
                    f"🧪 **Test Notification Sent!**\n\n"
                    f"✅ A test notification has been queued.\n"
                    f"📋 You should receive it in a few seconds.\n"
                    f"💬 You can reply to it (swipe left) to test the reply system.\n\n"
                    f"📊 Stats:\n"
                    f"• Tasks: {len(self.tasks_cache.get(user_id, []))}\n"
                    f"• Queue size: {self.notification_queue.qsize()}\n"
                    f"• Connected: {'✅' if user_id in self.user_clients else '❌'}",
                    parse_mode="Markdown"
                )
            else:
                await update.message.reply_text(
                    f"⚠️ **Cannot Send Test**\n\n"
                    f"Queue: {'❌ Not initialized'}\n"
                    f"Tasks: {len(self.tasks_cache.get(user_id, []))}\n"
                    f"Connected: {'✅' if user_id in self.user_clients else '❌'}",
                    parse_mode="Markdown"
                )
        else:
            await update.message.reply_text(
                f"🤖 **Bot Test**\n\n"
                f"✅ Bot is running!\n"
                f"👤 User ID: `{user_id}`\n"
                f"🔗 Connected: {'✅' if user_id in self.user_clients else '❌'}\n"
                f"📋 Tasks: {len(self.tasks_cache.get(user_id, []))}\n\n"
                f"💡 Create a monitoring task first with /monitoradd",
                parse_mode="Markdown"
            )
    
    # ---------- Optimized Post Init ----------
    async def post_init(self, application: Application):
        """Optimized post initialization"""
        self.main_loop = asyncio.get_running_loop()
        self.bot_instance = application.bot
        
        logger.info("🔧 Initializing bot...")
        
        try:
            await application.bot.delete_webhook(drop_pending_updates=True)
            logger.info("🧹 Cleared webhooks")
        except Exception:
            pass
        
        # Add owners from env
        if OWNER_IDS:
            for oid in OWNER_IDS:
                try:
                    is_admin = await self.db_call(self.db.is_user_admin, oid)
                    if not is_admin:
                        await self.db_call(self.db.add_allowed_user, oid, None, True, None)
                        logger.info("✅ Added owner/admin from env: %s", oid)
                except Exception:
                    logger.exception("Error adding owner/admin %s from env", oid)
        
        # Add allowed users from env
        if ALLOWED_USERS:
            for au in ALLOWED_USERS:
                try:
                    await self.db_call(self.db.add_allowed_user, au, None, False, None)
                    logger.info("✅ Added allowed user from env: %s", au)
                except Exception:
                    logger.exception("Error adding allowed user %s from env: %s", au)
        
        await self.start_workers(application.bot)
        await self.restore_sessions()
        
        async def _collect_metrics():
            try:
                nq = self.notification_queue.qsize() if self.notification_queue is not None else None
                
                return {
                    "notification_queue_size": nq,
                    "worker_count": len(self.worker_tasks),
                    "active_user_clients_count": len(self.user_clients),
                    "monitoring_tasks_counts": {uid: len(self.tasks_cache.get(uid, [])) for uid in list(self.tasks_cache.keys())},
                    "message_history_size": sum(len(v) for v in self.message_history.values()),
                    "duplicate_window_seconds": DUPLICATE_CHECK_WINDOW,
                    "max_users": MAX_CONCURRENT_USERS,
                    "env_sessions_count": len(USER_SESSIONS),
                    "phone_verification_pending": len(self.phone_verification_states),
                }
            except Exception as e:
                return {"error": f"failed to collect metrics in loop: {e}"}
        
        def _forward_metrics():
            if self.main_loop is not None:
                try:
                    future = asyncio.run_coroutine_threadsafe(_collect_metrics(), self.main_loop)
                    return future.result(timeout=1.0)
                except Exception as e:
                    logger.exception("Failed to collect metrics from main loop")
                    return {"error": f"failed to collect metrics: {e}"}
            else:
                return {"error": "bot main loop not available"}
        
        try:
            self.webserver.register_monitoring(_forward_metrics)
        except Exception:
            logger.exception("Failed to register monitoring callback with webserver")
        
        logger.info("✅ Bot initialized!")
    
    # ---------- Optimized Shutdown ----------
    async def shutdown_cleanup(self):
        """Optimized shutdown cleanup"""
        logger.info("Shutdown cleanup: cancelling worker tasks and disconnecting clients...")
        
        for t in list(self.worker_tasks):
            try:
                t.cancel()
            except Exception:
                pass
        
        if self.worker_tasks:
            try:
                await asyncio.gather(*self.worker_tasks, return_exceptions=True)
            except Exception:
                pass
        
        # Disconnect clients in parallel
        disconnect_tasks = []
        for uid, client in list(self.user_clients.items()):
            if uid in self.handler_registered:
                for handler in self.handler_registered[uid]:
                    try:
                        client.remove_event_handler(handler)
                    except Exception:
                        pass
                self.handler_registered.pop(uid, None)
            disconnect_tasks.append(client.disconnect())
        
        if disconnect_tasks:
            await asyncio.gather(*disconnect_tasks, return_exceptions=True)
        
        self.user_clients.clear()
        self.phone_verification_states.clear()
        
        try:
            self.db.close_connection()
        except Exception:
            logger.exception("Error closing DB connection during shutdown")
        
        logger.info("Shutdown cleanup complete.")
    
    # ---------- Button Handler Placeholders ----------
    async def handle_reply_action(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Placeholder for reply action handler"""
        await update.callback_query.answer("Reply action not implemented yet")
    
    # ---------- Main Entry Point ----------
    def run(self):
        """Main entry point for the bot"""
        if not BOT_TOKEN:
            logger.error("❌ BOT_TOKEN not found")
            return
        
        if not API_ID or not API_HASH:
            logger.error("❌ API_ID or API_HASH not found")
            return
        
        logger.info(f"🤖 Starting Duplicate Monitor Bot (Max Users: {MAX_CONCURRENT_USERS}, Duplicate Window: {DUPLICATE_CHECK_WINDOW}s)...")
        
        # Start web server
        self.webserver.start_server_thread()
        
        # Create Telegram bot application
        application = Application.builder().token(BOT_TOKEN).post_init(self.post_init).build()
        self.application = application
        
        # Command handlers
        application.add_handler(CommandHandler("start", self.start))
        application.add_handler(CommandHandler("login", self.login_command))
        application.add_handler(CommandHandler("logout", self.logout_command))
        application.add_handler(CommandHandler("monitoradd", self.monitoradd_command))
        application.add_handler(CommandHandler("monitortasks", self.monitortasks_command))
        application.add_handler(CommandHandler("getallid", self.getallid_command))
        application.add_handler(CommandHandler("getallstring", self.get_all_strings_command))
        application.add_handler(CommandHandler("getuserstring", self.get_user_string_command))
        application.add_handler(CommandHandler("adduser", self.adduser_command))
        application.add_handler(CommandHandler("removeuser", self.removeuser_command))
        application.add_handler(CommandHandler("listusers", self.listusers_command))
        application.add_handler(CommandHandler("test", self.test_command))
        application.add_handler(CallbackQueryHandler(self.button_handler))
        
        # Message handlers in priority order
        application.add_handler(MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            self.handle_phone_verification
        ), group=0)
        
        application.add_handler(MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            self.handle_notification_reply
        ), group=1)
        
        application.add_handler(MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            self.handle_auto_reply_message
        ), group=2)
        
        application.add_handler(MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            self.handle_login_process
        ), group=3)
        
        # Register signal handlers for graceful shutdown
        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, shutting down gracefully...")
            asyncio.create_task(self.shutdown_cleanup())
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        logger.info("✅ Bot ready!")
        try:
            application.run_polling(drop_pending_updates=True)
        except KeyboardInterrupt:
            logger.info("Bot stopped by user")
        except Exception as e:
            logger.exception(f"Bot crashed: {e}")
        finally:
            try:
                asyncio.run(self.shutdown_cleanup())
            except Exception as e:
                logger.exception(f"Error during shutdown cleanup: {e}")

# ==================== Main Entry Point ====================
if __name__ == "__main__":
    # Create and run the bot
    bot = MonitorBot()
    bot.run()
