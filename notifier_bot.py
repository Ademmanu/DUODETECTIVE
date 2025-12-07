#!/usr/bin/env python3
"""
notifier_bot.py - Telegram bot that receives duplicate alerts and lets human operator reply.

Behavior summary:
- Polls the broker (webserver or local DB) for pending alerts.
- Sends a notification message to configured OWNER_IDS (env) or allowed users from DB.
- Operator replies with a command: /reply <alert_id> <your text>
  Example: /reply 123 This is my reply
- When a reply is received, the bot posts it back to the broker (HTTP POST /api/replies or DB call).
- The monitor will later pick the reply up and deliver it into the original chat.

Environment variables:
- BOT_TOKEN (required)
- POLL_INTERVAL (seconds)
- BROKER_URL (optional) - if set, bot will use HTTP endpoints, else use DB directly
- OWNER_IDS (comma-separated) - who should receive alerts
"""

import os
import time
import logging
import asyncio
import requests
from typing import List, Optional
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

from database import get_db

logger = logging.getLogger("notifier")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
BROKER_URL = os.getenv("BROKER_URL", "")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "4"))
OWNER_IDS = set()
owner_env = os.getenv("OWNER_IDS", "")
if owner_env:
    for p in owner_env.split(","):
        try:
            OWNER_IDS.add(int(p.strip()))
        except Exception:
            pass

db = get_db()


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🔔 Duplicate Monitor Bot running. You will receive alerts here.")


async def reply_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /reply <alert_id> <your reply text>
    """
    user = update.effective_user
    user_id = user.id
    if OWNER_IDS and user_id not in OWNER_IDS:
        # check DB allowed users as fallback
        if not db.is_allowed(user_id):
            await update.message.reply_text("❌ You are not authorized to reply to alerts.")
            return

    text = update.message.text or ""
    parts = text.split(" ", 2)
    if len(parts) < 3:
        await update.message.reply_text("Usage: /reply <alert_id> <your reply text>\nExample: /reply 12 Sorry, please see pinned message.")
        return

    try:
        alert_id = int(parts[1])
    except ValueError:
        await update.message.reply_text("Invalid alert id. It must be a number.")
        return

    reply_text = parts[2].strip()
    if not reply_text:
        await update.message.reply_text("Reply text cannot be empty.")
        return

    # Submit reply to broker
    try:
        success = False
        if BROKER_URL:
            url = BROKER_URL.rstrip("/") + "/api/replies"
            resp = requests.post(url, json={"alert_id": alert_id, "reply_text": reply_text}, timeout=10)
            success = resp.status_code == 200
        else:
            success = db.mark_alert_replied(alert_id, reply_text)
        if success:
            await update.message.reply_text(f"✅ Reply submitted for alert {alert_id}. The monitor will deliver it shortly.")
        else:
            await update.message.reply_text("❌ Failed to submit reply. The alert may no longer be pending.")
    except Exception:
        logger.exception("Failed to submit reply")
        await update.message.reply_text("❌ Error submitting reply. Try again later.")


def format_alert(a: dict) -> str:
    created = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(a.get("created_at", int(time.time()))))
    text = a.get("message_text") or "<non-text>"
    return (
        f"🚨 Duplicate Found!\n\n"
        f"Alert ID: {a.get('id')}\n"
        f"Chat: `{a.get('chat_id')}`\n"
        f"Message ID: `{a.get('message_id')}`\n"
        f"Time: {created}\n\n"
        f"Message:\n`{(text[:800] + '...') if len(text) > 800 else text}`\n\n"
        "Reply with:\n/reply <alert_id> <your text>\n\n"
        "Example: /reply 12 Please ignore this duplicate."
    )


async def poll_broker_and_notify(application: Application):
    """
    Background loop that polls the broker for pending alerts and notifies owners.
    """
    logger.info("Starting broker poll loop (interval %s sec)", POLL_INTERVAL)
    while True:
        try:
            if BROKER_URL:
                url = BROKER_URL.rstrip("/") + "/api/alerts"
                resp = requests.get(url, timeout=10)
                alerts = resp.json().get("alerts", []) if resp.status_code == 200 else []
            else:
                alerts = db.get_pending_alerts(limit=50)

            if alerts:
                logger.info("Found %s pending alert(s)", len(alerts))

            # Compose recipients
            recipients: List[int] = []
            if OWNER_IDS:
                recipients = list(OWNER_IDS)
            else:
                allowed = db.get_allowed_users()
                recipients = [u["user_id"] for u in allowed]

            for a in alerts:
                msg = format_alert(a)
                for uid in recipients:
                    try:
                        # send message via bot
                        await application.bot.send_message(uid, msg, parse_mode="Markdown")
                    except Exception:
                        logger.exception("Failed to send alert %s to %s", a.get("id"), uid)
            # Sleep controlled interval
            await asyncio.sleep(POLL_INTERVAL)
        except Exception:
            logger.exception("Error while polling broker")
            await asyncio.sleep(POLL_INTERVAL)


def main():
    if not BOT_TOKEN:
        logger.error("BOT_TOKEN env required")
        return

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("reply", reply_command))

    # Start background polling once the bot starts running
    async def _on_startup(app_obj: Application):
        # launch polling task
        app_obj.create_task(poll_broker_and_notify(app_obj))

    app.post_init = _on_startup

    logger.info("Starting notifier bot...")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()