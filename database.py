"""
Lightweight SQLite broker for Duplicate Message Monitor system.

This database module stores:
- monitored messages (to detect duplicates)
- alerts created when duplicates are found
- replies from human operator
- simple monitoring tasks and allowed users

It is intentionally small and dependency-free (only stdlib sqlite3).
"""

import sqlite3
import threading
import json
import os
import time
from typing import Optional, List, Dict, Any
from datetime import datetime

DB_LOCK = threading.Lock()


class Database:
    def __init__(self, path: str = "duomonitor.db"):
        self.path = path
        self._ensure_dir()
        self._conn = sqlite3.connect(self.path, check_same_thread=False, timeout=30)
        self._conn.row_factory = sqlite3.Row
        self._init_schema()

    def _ensure_dir(self):
        d = os.path.dirname(self.path)
        if d and not os.path.exists(d):
            try:
                os.makedirs(d, exist_ok=True)
            except Exception:
                pass

    def _init_schema(self):
        with DB_LOCK:
            cur = self._conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER NOT NULL,
                    message_id INTEGER NOT NULL,
                    message_hash TEXT NOT NULL,
                    message_text TEXT,
                    sender_id INTEGER,
                    ts INTEGER NOT NULL,
                    is_duplicate INTEGER DEFAULT 0,
                    first_seen_message_id INTEGER
                );
                """
            )

            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_messages_hash_chat ON messages (chat_id, message_hash);
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS alerts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    alert_uuid TEXT,
                    created_at INTEGER NOT NULL,
                    chat_id INTEGER NOT NULL,
                    message_id INTEGER NOT NULL,
                    message_text TEXT,
                    message_hash TEXT,
                    monitor_info TEXT,
                    status TEXT DEFAULT 'pending', -- pending, replied, delivered
                    reply_text TEXT,
                    replied_at INTEGER,
                    delivered_at INTEGER
                );
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS monitor_tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    label TEXT,
                    owner_id INTEGER,
                    target_ids TEXT, -- JSON array
                    is_active INTEGER DEFAULT 1,
                    created_at INTEGER DEFAULT (strftime('%s','now'))
                );
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS allowed_users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    is_owner INTEGER DEFAULT 0,
                    added_at INTEGER DEFAULT (strftime('%s','now'))
                );
                """
            )

            self._conn.commit()

    # ----------------- Message ingestion & duplicate detection -----------------
    def save_message(self, chat_id: int, message_id: int, message_hash: str, message_text: Optional[str], sender_id: Optional[int]) -> Dict[str, Any]:
        """
        Save a seen message and determine whether it's a duplicate.

        Returns a dict:
        {
            "is_duplicate": bool,
            "first_seen_message_id": Optional[int],
        }
        """
        ts = int(time.time())
        with DB_LOCK:
            cur = self._conn.cursor()
            # Look for existing identical message in same chat
            cur.execute(
                "SELECT id, message_id FROM messages WHERE chat_id = ? AND message_hash = ? ORDER BY id ASC LIMIT 1",
                (chat_id, message_hash),
            )
            row = cur.fetchone()
            if row:
                # duplicate found
                first_message_id = row["message_id"]
                cur.execute(
                    "INSERT INTO messages (chat_id, message_id, message_hash, message_text, sender_id, ts, is_duplicate, first_seen_message_id) VALUES (?, ?, ?, ?, ?, ?, 1, ?)",
                    (chat_id, message_id, message_hash, message_text, sender_id, ts, first_message_id),
                )
                self._conn.commit()
                return {"is_duplicate": True, "first_seen_message_id": first_message_id}
            else:
                # new message
                cur.execute(
                    "INSERT INTO messages (chat_id, message_id, message_hash, message_text, sender_id, ts, is_duplicate, first_seen_message_id) VALUES (?, ?, ?, ?, ?, ?, 0, NULL)",
                    (chat_id, message_id, message_hash, message_text, sender_id, ts),
                )
                self._conn.commit()
                return {"is_duplicate": False, "first_seen_message_id": None}

    # ----------------- Alerts -----------------
    def add_alert(self, alert_uuid: Optional[str], chat_id: int, message_id: int, message_text: Optional[str], message_hash: str, monitor_info: Optional[Dict] = None) -> int:
        """
        Create an alert when a duplicate is detected.
        Returns the alert id.
        """
        ts = int(time.time())
        monitor_json = json.dumps(monitor_info or {})
        with DB_LOCK:
            cur = self._conn.cursor()
            cur.execute(
                "INSERT INTO alerts (alert_uuid, created_at, chat_id, message_id, message_text, message_hash, monitor_info, status) VALUES (?, ?, ?, ?, ?, ?, ?, 'pending')",
                (alert_uuid, ts, chat_id, message_id, message_text, message_hash, monitor_json),
            )
            aid = cur.lastrowid
            self._conn.commit()
            return aid

    def get_pending_alerts(self, limit: int = 50) -> List[Dict[str, Any]]:
        with DB_LOCK:
            cur = self._conn.cursor()
            cur.execute("SELECT * FROM alerts WHERE status = 'pending' ORDER BY created_at ASC LIMIT ?", (limit,))
            rows = cur.fetchall()
            return [dict(r) for r in rows]

    def mark_alert_replied(self, alert_id: int, reply_text: str) -> bool:
        ts = int(time.time())
        with DB_LOCK:
            cur = self._conn.cursor()
            cur.execute("UPDATE alerts SET status = 'replied', reply_text = ?, replied_at = ? WHERE id = ? AND status = 'pending'", (reply_text, ts, alert_id))
            updated = cur.rowcount > 0
            self._conn.commit()
            return updated

    def get_replied_alerts(self, limit: int = 50) -> List[Dict[str, Any]]:
        with DB_LOCK:
            cur = self._conn.cursor()
            cur.execute("SELECT * FROM alerts WHERE status = 'replied' ORDER BY replied_at ASC LIMIT ?", (limit,))
            rows = cur.fetchall()
            return [dict(r) for r in rows]

    def mark_alert_delivered(self, alert_id: int) -> bool:
        ts = int(time.time())
        with DB_LOCK:
            cur = self._conn.cursor()
            cur.execute("UPDATE alerts SET status = 'delivered', delivered_at = ? WHERE id = ? AND status = 'replied'", (ts, alert_id))
            updated = cur.rowcount > 0
            self._conn.commit()
            return updated

    # ----------------- Tasks -----------------
    def add_monitor_task(self, label: str, owner_id: int, target_ids: List[int]) -> int:
        with DB_LOCK:
            cur = self._conn.cursor()
            cur.execute(
                "INSERT INTO monitor_tasks (label, owner_id, target_ids, is_active) VALUES (?, ?, ?, 1)",
                (label, owner_id, json.dumps(target_ids)),
            )
            tid = cur.lastrowid
            self._conn.commit()
            return tid

    def get_active_tasks(self) -> List[Dict[str, Any]]:
        with DB_LOCK:
            cur = self._conn.cursor()
            cur.execute("SELECT * FROM monitor_tasks WHERE is_active = 1 ORDER BY created_at DESC")
            rows = cur.fetchall()
            results = []
            for r in rows:
                rec = dict(r)
                try:
                    rec["target_ids"] = json.loads(rec.get("target_ids") or "[]")
                except Exception:
                    rec["target_ids"] = []
                results.append(rec)
            return results

    def remove_monitor_task(self, task_id: int) -> bool:
        with DB_LOCK:
            cur = self._conn.cursor()
            cur.execute("DELETE FROM monitor_tasks WHERE id = ?", (task_id,))
            deleted = cur.rowcount > 0
            self._conn.commit()
            return deleted

    # ----------------- Allowed / Owner users -----------------
    def add_allowed_user(self, user_id: int, username: Optional[str] = None, is_owner: bool = False) -> bool:
        with DB_LOCK:
            cur = self._conn.cursor()
            try:
                cur.execute("INSERT INTO allowed_users (user_id, username, is_owner) VALUES (?, ?, ?)", (user_id, username, 1 if is_owner else 0))
                self._conn.commit()
                return True
            except sqlite3.IntegrityError:
                return False

    def remove_allowed_user(self, user_id: int) -> bool:
        with DB_LOCK:
            cur = self._conn.cursor()
            cur.execute("DELETE FROM allowed_users WHERE user_id = ?", (user_id,))
            deleted = cur.rowcount > 0
            self._conn.commit()
            return deleted

    def get_allowed_users(self) -> List[Dict[str, Any]]:
        with DB_LOCK:
            cur = self._conn.cursor()
            cur.execute("SELECT user_id, username, is_owner FROM allowed_users ORDER BY added_at DESC")
            rows = cur.fetchall()
            return [dict(r) for r in rows]

    def is_allowed(self, user_id: int) -> bool:
        with DB_LOCK:
            cur = self._conn.cursor()
            cur.execute("SELECT 1 FROM allowed_users WHERE user_id = ?", (user_id,))
            return cur.fetchone() is not None

    def close(self):
        try:
            self._conn.close()
        except Exception:
            pass


# Provide a module-level DB object for convenience
_db = None


def get_db(path: Optional[str] = None) -> Database:
    global _db
    if _db is None:
        _db = Database(path or os.getenv("DUO_DB_PATH", "duomonitor.db"))
    return _db