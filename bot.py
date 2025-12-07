# bot.py - Simplified version that just provides status
from aiogram import Bot, Dispatcher, types
from aiogram.contrib.middlewares.logging import LoggingMiddleware
from aiogram.utils import executor
import logging
from config import BOT_TOKEN, YOUR_USER_ID

# Setup
logging.basicConfig(level=logging.INFO)
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)
dp.middleware.setup(LoggingMiddleware())

@dp.message_handler(commands=['start', 'help'])
async def send_welcome(message: types.Message):
    await message.reply(
        "🤖 **Telegram Duplicate Monitor Bot**\n\n"
        "I work with the monitoring system to alert you about duplicate messages.\n\n"
        "**Commands:**\n"
        "/status - Check system status\n"
        "/test - Send a test alert\n"
        "/chats - Show monitored chats (coming soon)"
    )

@dp.message_handler(commands=['status'])
async def check_status(message: types.Message):
    from database import db
    try:
        alerts = db.get_pending_alerts()
        await message.reply(
            f"✅ **System Status:** Running\n"
            f"📊 **Pending alerts:** {len(alerts)}\n"
            f"👤 **Your ID:** {YOUR_USER_ID}"
        )
    except Exception as e:
        await message.reply(f"❌ Error: {str(e)}")

@dp.message_handler(commands=['test'])
async def send_test_alert(message: types.Message):
    test_msg = """
🚨 **TEST ALERT**
This is a test alert from your duplicate monitor system.
Reply with `/reply 999 Test response` to test the reply system.
    """
    await bot.send_message(YOUR_USER_ID, test_msg)
    await message.reply("✅ Test alert sent to your PM!")

async def start_bot():
    """Start the bot polling"""
    print("🤖 Bot starting...")
    try:
        await dp.start_polling()
    except Exception as e:
        print(f"❌ Bot error: {e}")

# For direct execution
if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True)
