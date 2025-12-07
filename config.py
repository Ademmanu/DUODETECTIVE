# config.py
import os
from typing import Set

# Bot Configuration
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")

# Admin/Allowed Users
ADMIN_IDS: Set[int] = set()
admin_env = os.getenv("ADMIN_IDS", "").strip()
if admin_env:
    for part in admin_env.split(","):
        part = part.strip()
        if part.isdigit():
            ADMIN_IDS.add(int(part))

# Redis configuration (optional, for queue)
REDIS_URL = os.getenv("REDIS_URL", "")

# Monitoring settings
DUPLICATE_TIME_WINDOW = int(os.getenv("DUPLICATE_TIME_WINDOW", "3600"))  # 1 hour
MAX_MESSAGE_LENGTH = int(os.getenv("MAX_MESSAGE_LENGTH", "4096"))

# Web server
WEB_SERVER_HOST = os.getenv("WEB_SERVER_HOST", "0.0.0.0")
WEB_SERVER_PORT = int(os.getenv("WEB_SERVER_PORT", "5000"))
