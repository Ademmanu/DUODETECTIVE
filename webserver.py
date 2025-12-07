"""
Lightweight Flask broker for alerts and replies.

Provides HTTP API endpoints for:
- POST /api/alerts    -> create an alert (monitor -> broker)
- GET  /api/alerts    -> list pending alerts (notifier polls)
- POST /api/replies   -> submit a reply for an alert (notifier -> broker)
- GET  /api/replied_alerts -> list alerts which have been replied (monitor polls)
- POST /api/alerts/<id>/delivered -> mark alert delivered (monitor -> broker)

This server re-uses database.py so it can be run alongside the monitor or notifier using the same DB file.
"""

from flask import Flask, request, jsonify
import logging
import os
import threading
import time
from database import get_db

logger = logging.getLogger("broker")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

app = Flask(__name__)
db = get_db()


@app.route("/api/alerts", methods=["POST"])
def create_alert():
    data = request.get_json(silent=True) or {}
    try:
        chat_id = int(data.get("chat_id"))
        message_id = int(data.get("message_id"))
        message_text = data.get("message_text")
        message_hash = data.get("message_hash", "")
        alert_uuid = data.get("alert_uuid")
        monitor_info = data.get("monitor_info", {})
    except Exception:
        return jsonify({"error": "invalid payload"}), 400

    try:
        aid = db.add_alert(alert_uuid, chat_id, message_id, message_text, message_hash, monitor_info)
        return jsonify({"status": "ok", "id": aid}), 200
    except Exception:
        logger.exception("Failed to create alert")
        return jsonify({"error": "internal"}), 500


@app.route("/api/alerts", methods=["GET"])
def list_pending_alerts():
    try:
        alerts = db.get_pending_alerts(limit=200)
        return jsonify({"status": "ok", "alerts": alerts}), 200
    except Exception:
        logger.exception("Failed to list alerts")
        return jsonify({"error": "internal"}), 500


@app.route("/api/replies", methods=["POST"])
def post_reply():
    data = request.get_json(silent=True) or {}
    try:
        alert_id = int(data.get("alert_id"))
        reply_text = data.get("reply_text", "")
    except Exception:
        return jsonify({"error": "invalid payload"}), 400

    if not reply_text:
        return jsonify({"error": "empty reply"}), 400

    try:
        ok = db.mark_alert_replied(alert_id, reply_text)
        return jsonify({"status": "ok", "applied": bool(ok)}), 200
    except Exception:
        logger.exception("Failed to save reply")
        return jsonify({"error": "internal"}), 500


@app.route("/api/replied_alerts", methods=["GET"])
def get_replied_alerts():
    try:
        alerts = db.get_replied_alerts(limit=200)
        return jsonify({"status": "ok", "alerts": alerts}), 200
    except Exception:
        logger.exception("Failed to fetch replied alerts")
        return jsonify({"error": "internal"}), 500


@app.route("/api/alerts/<int:aid>/delivered", methods=["POST"])
def mark_delivered(aid: int):
    try:
        ok = db.mark_alert_delivered(aid)
        return jsonify({"status": "ok", "applied": bool(ok)}), 200
    except Exception:
        logger.exception("Failed to mark delivered")
        return jsonify({"error": "internal"}), 500


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "ts": int(time.time())}), 200


def run_server(host: str = "0.0.0.0", port: int = 5000):
    logger.info("Starting broker webserver on %s:%s", host, port)
    app.run(host=host, port=port, debug=False, use_reloader=False, threaded=True)


if __name__ == "__main__":
    run_server()