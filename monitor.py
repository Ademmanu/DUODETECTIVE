#!/usr/bin/env python3
"""
monitor.py - Telegram userbot that watches chats and detects duplicate messages.

Behavior summary:
- Requires a Telethon session string (MONITOR_SESSION) or will attempt to create one if PHONE/LOGIN flow provided.
- Monitors chats specified by environment variable MONITOR_CHAT_IDS (comma-separated chat ids).
- Stores messages in database.py and detects duplicates using sha256 hash of message text.
- When a duplicate is detected, creates an alert in the shared broker (via HTTP BROKER_URL or direct DB).
- Periodically polls broker for replies to alerts and sends your manual replies back to the originating chat, as a reply to the original message.

Environment variables:
- API_ID, API_HASH
- MONITOR_SESSION (preferred)
- MONITOR_CHAT_IDS (comma-separated integers)
- BROKER_URL (optional) - if set, monitor will POST/GET to this webserver. Otherwise uses DB directly.
"""

import os
import asyncio
import logging
import hashlib
import json
import time
from typing import Optional, List
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from database import get_db

logger = logging.getLogger("monitor")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
MONITOR_SESSION = os.getenv("MONITOR_SESSION", "")
MONITOR_CHAT_IDS = os.getenv("MONITOR_CHAT_IDS", "")  # e.g. "-1001234, 12345678"
BROKER_URL = os.getenv("BROKER_URL", "")  # e.g. https://your-webserver.example.com

POLL_REPLY_INTERVAL = int(os.getenv("POLL_REPLY_INTERVAL", "5"))  # seconds

db = get_db()


def compute_hash(text: Optional[str]) -> str:
    if text is None:
        text = ""
    # Normalize whitespace and strip
    norm = " ".join(text.split())
    return hashlib.sha256(norm.encode("utf-8")).hexdigest()


async def send_alert_to_broker(alert: dict) -> Optional[int]:
    """
    If BROKER_URL is set, POST to /api/alerts, else create alert directly in DB.
    Returns alert id or None on failure.
    """
    try:
        if BROKER_URL:
            import requests  # local import to avoid requirement when unused
            url = BROKER_URL.rstrip("/") + "/api/alerts"
            resp = requests.post(url, json=alert, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                return int(data.get("id")) if data.get("id") else None
            else:
                logger.warning("Broker returned %s: %s", resp.status_code, resp.text)
                return None
        else:
            return db.add_alert(
                alert_uuid=alert.get("alert_uuid"),
                chat_id=alert["chat_id"],
                message_id=alert["message_id"],
                message_text=alert.get("message_text"),
                message_hash=alert.get("message_hash"),
                monitor_info=alert.get("monitor_info"),
            )
    except Exception:
        logger.exception("Failed to send alert to broker")
        return None


async def poll_and_delivery_loop(client: TelegramClient):
    """
    Background task: poll for replies from broker and deliver them as replies in the original chat.
    """
    logger.info("Starting poll_and_delivery_loop (poll interval %s sec)", POLL_REPLY_INTERVAL)
    while True:
        try:
            if BROKER_URL:
                import requests
                url = BROKER_URL.rstrip("/") + "/api/replied_alerts"
                resp = requests.get(url, timeout=10)
                alerts = resp.json().get("alerts", []) if resp.status_code == 200 else []
            else:
                alerts = db.get_replied_alerts(limit=50)

            for a in alerts:
                alert_id = int(a["id"])
                chat_id = int(a["chat_id"])
                message_id = int(a["message_id"])
                reply_text = a.get("reply_text") or ""
                # Send reply as the logged-in user
                try:
                    await client.send_message(chat_id, reply_text, reply_to=message_id)
                    logger.info("Delivered reply for alert %s to chat %s message %s", alert_id, chat_id, message_id)
                    # Mark delivered
                    if BROKER_URL:
                        try:
                            url2 = BROKER_URL.rstrip("/") + f"/api/alerts/{alert_id}/delivered"
                            r2 = requests.post(url2, timeout=5)
                        except Exception:
                            logger.exception("Failed to mark delivered on broker")
                    else:
                        db.mark_alert_delivered(alert_id)
                except Exception:
                    logger.exception("Failed to deliver reply for alert %s", alert_id)
            await asyncio.sleep(POLL_REPLY_INTERVAL)
        except Exception:
            logger.exception("Error in poll_and_delivery_loop")
            await asyncio.sleep(POLL_REPLY_INTERVAL)


async def main():
    if not API_ID or not API_HASH:
        logger.error("API_ID and API_HASH must be set in environment")
        return

    if not MONITOR_SESSION:
        logger.error("MONITOR_SESSION is required. Please provide a saved Telethon session string in MONITOR_SESSION env.")
        return

    session = MONITOR_SESSION.strip()
    client = TelegramClient(StringSession(session), API_ID, API_HASH)
    await client.connect()
    logger.info("Monitor connected as session")

    target_ids = []
    if MONITOR_CHAT_IDS:
        for part in MONITOR_CHAT_IDS.split(","):
            try:
                if part.strip():
                    target_ids.append(int(part.strip()))
            except Exception:
                logger.warning("Skipping invalid MONITOR_CHAT_IDS entry: %s", part)

    # If there are monitor tasks stored in DB, merge their target ids
    try:
        tasks = db.get_active_tasks()
        for t in tasks:
            try:
                for tid in t.get("target_ids", []):
                    if tid not in target_ids:
                        target_ids.append(int(tid))
            except Exception:
                continue
    except Exception:
        logger.exception("Failed to read monitor tasks from DB")

    logger.info("Monitoring chats: %s", target_ids)

    @client.on(events.NewMessage())
    async def handler(event):
        try:
            msg = event.message
            if not msg or msg.text is None:
                return
            chat_id = int(getattr(event.chat_id, "__int__", lambda: event.chat_id)() if hasattr(event, "chat_id") else msg.chat_id or msg.to_id.channel_id or 0)
            # If we have a set of target ids, skip others
            if target_ids and (chat_id not in target_ids):
                return

            text = msg.text or ""
            sender_id = getattr(msg.from_id, "user_id", None) if msg.from_id else None
            message_id = msg.id
            message_hash = compute_hash(text)

            res = db.save_message(chat_id, message_id, message_hash, text, sender_id)
            if res.get("is_duplicate"):
                logger.info("Duplicate detected in chat %s message %s (first seen %s)", chat_id, message_id, res.get("first_seen_message_id"))
                alert = {
                    "alert_uuid": None,
                    "chat_id": chat_id,
                    "message_id": message_id,
                    "message_text": text,
                    "message_hash": message_hash,
                    "monitor_info": {"detected_at": int(time.time()), "first_seen_message_id": res.get("first_seen_message_id")},
                }
                aid = await send_alert_to_broker(alert)
                logger.info("Alert created: %s", aid)
        except Exception:
            logger.exception("Error in message handler")

    # Start the poll/delivery loop
    asyncio.create_task(poll_and_delivery_loop(client))

    logger.info("Monitor up and running. Press Ctrl+C to stop.")
    try:
        await client.run_until_disconnected()
    finally:
        await client.disconnect()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Monitor interrupted by user")