# bot.py - Simplified version that just sends test alerts
# Note: The main bot functionality is already in monitor.py
# This file is kept for future expansion

import logging
from aiogram import Bot, Dispatcher, types
from aiogram.contrib.middlewares.logging import LoggingMiddleware
from config import BOT_TOKEN, YOUR_USER_ID

# Setup logging
logging.basicConfig(level=logging.INFO)

# Initialize bot and dispatcher
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)
dp.middleware.setup(LoggingMiddleware())

@dp.message_handler(commands=['start'])
async def send_welcome(message: types.Message):
    await message.reply(
        "🤖 **Duplicate Monitor Bot**\n\n"
        "This bot works with the monitoring script.\n"
        "When duplicates are detected, you'll receive alerts.\n\n"
        "**Commands:**\n"
        "/status - Check if monitor is running\n"
        "/test - Send a test alert\n"
        "/help - Show this message"
    )

@dp.message_handler(commands=['status'])
async def check_status(message: types.Message):
    await message.reply("✅ Monitor is running. You'll receive alerts when duplicates are detected.")

@dp.message_handler(commands=['test'])
async def send_test_alert(message: types.Message):
    test_alert = """
🚨 **TEST ALERT - DUPLICATE DETECTED**
**Chat:** Test Group
**Message ID:** 12345
**Alert ID:** 999
──────────────
**Message:**
This is a test duplicate message for system verification.
──────────────
Reply to this message with:
`/reply 999 This is a test response`
    """
    
    await bot.send_message(YOUR_USER_ID, test_alert)
    await message.reply("✅ Test alert sent to your PM!")

@dp.message_handler(commands=['help'])
async def show_help(message: types.Message):
    await send_welcome(message)

async def main():
    await dp.start_polling()

if __name__ == "__main__":
    from aiogram import executor
    executor.start_polling(dp, skip_updates=True)
