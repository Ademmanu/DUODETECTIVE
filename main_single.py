#!/usr/bin/env python3
"""
Single-process deployment running:
- aiohttp-based broker HTTP API (binds to $PORT)
- Telethon monitor (user account) that detects duplicates
- Notifier using Telegram Bot HTTP API (long-polling getUpdates)
- Delivery loop that sends operator replies back to chats

Set these environment variables in Render:
- PORT (Render provides it automatically)
- API_ID, API_HASH
- MONITOR_SESSION (Telethon StringSession for the user account)
- MONITOR_CHAT_IDS (optional, comma-separated)
- BOT_TOKEN (Telegram bot token)
- OWNER_IDS (comma-separated user IDs who get alerts)
Optional tuning:
- POLL_INTERVAL (seconds for notifier scanning DB) default=4
- POLL_REPLY_INTERVAL (seconds for monitor to deliver replies) default=5
"""
import os
import asyncio
import logging
import hashlib
import json
import time
from typing import List, Optional
from aiohttp import web, ClientSession
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from database import get_db

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("duo_single")

# Env / config
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
MONITOR_SESSION = os.getenv("MONITOR_SESSION", "")
MONITOR_CHAT_IDS = os.getenv("MONITOR_CHAT_IDS", "")  # comma-separated list
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
OWNER_IDS_RAW = os.getenv("OWNER_IDS", "")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "4"))
POLL_REPLY_INTERVAL = int(os.getenv("POLL_REPLY_INTERVAL", "5"))
HTTP_PORT = int(os.getenv("PORT", "5000"))

# parse owners and monitors
OWNER_IDS = []
if OWNER_IDS_RAW:
    for p in OWNER_IDS_RAW.split(","):
        try:
            OWNER_IDS.append(int(p.strip()))
        except Exception:
            pass

MONITOR_CHAT_IDS_SET = set()
if MONITOR_CHAT_IDS:
    for p in MONITOR_CHAT_IDS.split(","):
        try:
            MONITOR_CHAT_IDS_SET.add(int(p.strip()))
        except Exception:
            pass

db = get_db()  # uses the Database from database.py

# --- helpers ---
def compute_hash(text: Optional[str]) -> str:
    if text is None:
        text = ""
    norm = " ".join(text.split())
    return hashlib.sha256(norm.encode("utf-8")).hexdigest()

async def run_db(func, *args, **kwargs):
    return await asyncio.to_thread(func, *args, **kwargs)

# --- aiohttp broker app (basic) ---
async def create_broker_app():
    app = web.Application()
    routes = web.RouteTableDef()

    @routes.get("/health")
    async def health(request):
        return web.json_response({"status": "ok", "ts": int(time.time())})

    @routes.post("/api/alerts")
    async def create_alert(request):
        data = await request.json()
        try:
            chat_id = int(data["chat_id"])
            message_id = int(data["message_id"])
            message_text = data.get("message_text")
            message_hash = data.get("message_hash", "")
        except Exception:
            return web.json_response({"error": "invalid payload"}, status=400)
        aid = await run_db(db.add_alert, data.get("alert_uuid"), chat_id, message_id, message_text, message_hash, data.get("monitor_info"))
        return web.json_response({"status": "ok", "id": aid})

    @routes.get("/api/alerts")
    async def list_pending(request):
        alerts = await run_db(db.get_pending_alerts, 200)
        return web.json_response({"status": "ok", "alerts": alerts})

    @routes.post("/api/replies")
    async def post_reply(request):
        data = await request.json()
        try:
            alert_id = int(data["alert_id"])
            reply_text = str(data["reply_text"])
        except Exception:
            return web.json_response({"error": "invalid payload"}, status=400)
        ok = await run_db(db.mark_alert_replied, alert_id, reply_text)
        return web.json_response({"status": "ok", "applied": bool(ok)})

    @routes.get("/api/replied_alerts")
    async def get_replied(request):
        alerts = await run_db(db.get_replied_alerts, 200)
        return web.json_response({"status": "ok", "alerts": alerts})

    @routes.post("/api/alerts/{aid}/delivered")
    async def mark_delivered(request):
        aid = int(request.match_info["aid"])
        ok = await run_db(db.mark_alert_delivered, aid)
        return web.json_response({"status": "ok", "applied": bool(ok)})

    app.add_routes(routes)
    return app

# --- Telegram Bot HTTP helpers (simple) ---
BOT_API_BASE = f"https://api.telegram.org/bot{BOT_TOKEN}"

async def bot_send_message(session: ClientSession, chat_id: int, text: str):
    url = BOT_API_BASE + "/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
    try:
        async with session.post(url, json=payload, timeout=15) as resp:
            return await resp.json()
    except Exception:
        logger.exception("Bot send_message failed")
        return None

# --- notifier: polls DB.pending alerts and notifies owners ---
async def notifier_loop():
    logger.info("Notifier loop started")
    async with ClientSession() as session:
        while True:
            try:
                alerts = await run_db(db.get_pending_alerts, 200)
                if alerts:
                    logger.info("Notifier found %d pending alert(s)", len(alerts))
                recipients = OWNER_IDS
                if not recipients:
                    allowed = await run_db(db.get_allowed_users)
                    recipients = [u["user_id"] for u in allowed]
                for a in alerts:
                    # craft message
                    created = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(a.get("created_at", int(time.time()))))
                    text = a.get("message_text") or "<non-text>"
                    msg = (
                        f"🚨 Duplicate Found!\n\n"
                        f"Alert ID: {a.get('id')}\n"
                        f"Chat: `{a.get('chat_id')}`\n"
                        f"Message ID: `{a.get('message_id')}`\n"
                        f"Time: {created}\n\n"
                        f"Message:\n`{(text[:800] + '...') if len(text) > 800 else text}`\n\n"
                        "Reply with:\n/reply <alert_id> <your text>\n\n"
                        "Example: /reply 12 Please ignore this duplicate."
                    )
                    for uid in recipients:
                        try:
                            await bot_send_message(session, uid, msg)
                        except Exception:
                            logger.exception("Failed to notify %s about alert %s", uid, a.get("id"))
                # Also poll bot updates (getUpdates) to capture /reply commands
                # We use getUpdates long polling simple approach to avoid heavy libs.
                await poll_bot_updates_and_apply(session)
            except Exception:
                logger.exception("Error in notifier_loop")
            await asyncio.sleep(POLL_INTERVAL)

# --- poll getUpdates to capture /reply commands ---
_getupdates_offset = 0
async def poll_bot_updates_and_apply(session: ClientSession):
    global _getupdates_offset
    if not BOT_TOKEN:
        return
    try:
        url = BOT_API_BASE + "/getUpdates"
        params = {"timeout": 10, "offset": _getupdates_offset, "allowed_updates": ["message"]}
        async with session.get(url, params=params, timeout=35) as resp:
            data = await resp.json()
        if not data.get("ok"):
            return
        for upd in data.get("result", []):
            _getupdates_offset = upd["update_id"] + 1
            msg = upd.get("message")
            if not msg:
                continue
            text = msg.get("text", "").strip()
            from_id = msg.get("from", {}).get("id")
            # simple auth: owner list or DB allowed users
            allowed = False
            if OWNER_IDS and from_id in OWNER_IDS:
                allowed = True
            else:
                try:
                    allowed = await run_db(db.is_allowed, from_id)
                except Exception:
                    allowed = False
            if not allowed:
                # ignore
                continue
            if text.startswith("/reply"):
                parts = text.split(" ", 2)
                if len(parts) < 3:
                    await bot_send_message(session, from_id, "Usage: /reply <alert_id> <your text>")
                    continue
                try:
                    alert_id = int(parts[1])
                except Exception:
                    await bot_send_message(session, from_id, "Invalid alert id. Must be a number.")
                    continue
                reply_text = parts[2].strip()
                if not reply_text:
                    await bot_send_message(session, from_id, "Empty reply text.")
                    continue
                ok = await run_db(db.mark_alert_replied, alert_id, reply_text)
                if ok:
                    await bot_send_message(session, from_id, f"✅ Reply submitted for alert {alert_id}.")
                else:
                    await bot_send_message(session, from_id, f"❌ Could not apply reply for alert {alert_id}. It may not be pending.")
    except Exception:
        logger.exception("Error polling bot updates")

# --- monitor: Telethon client watches chats and stores messages ---
async def start_monitor_and_delivery(loop):
    if not API_ID or not API_HASH or not MONITOR_SESSION:
        logger.error("API_ID/API_HASH/MONITOR_SESSION must be set for monitor")
        return

    client = TelegramClient(StringSession(MONITOR_SESSION), API_ID, API_HASH)
    await client.connect()
    logger.info("Telethon client connected (monitor)")

    # add event handler
    @client.on(events.NewMessage)
    async def _handler(event):
        try:
            msg = event.message
            if not msg or msg.text is None:
                return
            chat_id = int(event.chat_id or msg.chat_id or 0)
            if MONITOR_CHAT_IDS_SET and (chat_id not in MONITOR_CHAT_IDS_SET):
                return
            text = msg.text or ""
            sender = getattr(msg.from_id, "user_id", None) if msg.from_id else None
            message_id = msg.id
            mhash = compute_hash(text)
            res = await run_db(db.save_message, chat_id, message_id, mhash, text, sender)
            if res.get("is_duplicate"):
                logger.info("Duplicate detected chat=%s message=%s", chat_id, message_id)
                alert = {
                    "alert_uuid": None,
                    "chat_id": chat_id,
                    "message_id": message_id,
                    "message_text": text,
                    "message_hash": mhash,
                    "monitor_info": {"detected_at": int(time.time()), "first_seen_message_id": res.get("first_seen_message_id")}
                }
                await run_db(db.add_alert, alert.get("alert_uuid"), chat_id, message_id, text, mhash, alert.get("monitor_info"))
        except Exception:
            logger.exception("Error in Telethon handler")

    # Delivery loop: pick replied alerts and send replies back as your user account
    async def delivery_loop():
        logger.info("Delivery loop started")
        while True:
            try:
                replied = await run_db(db.get_replied_alerts, 200)
                if replied:
                    logger.info("Found %d replied alert(s) to deliver", len(replied))
                for a in replied:
                    aid = int(a["id"])
                    chat_id = int(a["chat_id"])
                    message_id = int(a["message_id"])
                    reply_text = a.get("reply_text") or ""
                    try:
                        # send reply as your user account
                        await client.send_message(chat_id, reply_text, reply_to=message_id)
                        logger.info("Delivered reply for alert %s", aid)
                        await run_db(db.mark_alert_delivered, aid)
                    except Exception:
                        logger.exception("Failed delivering reply for alert %s", aid)
                await asyncio.sleep(POLL_REPLY_INTERVAL)
            except Exception:
                logger.exception("Error in delivery_loop")
                await asyncio.sleep(POLL_REPLY_INTERVAL)

    # start delivery loop task
    asyncio.create_task(delivery_loop())

    # keep Telethon running until program exit
    try:
        await client.run_until_disconnected()
    finally:
        await client.disconnect()

# --- main entrypoint ---
async def main():
    # start aiohttp broker
    app = await create_broker_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", HTTP_PORT)
    await site.start()
    logger.info("Broker web server started on port %s", HTTP_PORT)

    # Start notifier loop (background)
    asyncio.create_task(notifier_loop())

    # Start monitor + delivery
    await start_monitor_and_delivery(asyncio.get_running_loop())

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down")