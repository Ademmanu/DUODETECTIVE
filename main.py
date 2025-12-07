# main.py - Single entry point for Render
import asyncio
import threading
import multiprocessing
import sys
import os
from time import sleep
import signal

def run_monitor():
    """Run monitor in a separate thread"""
    print("🚀 Starting Monitor...")
    from monitor import main as monitor_main
    asyncio.run(monitor_main())

def run_bot():
    """Run bot in a separate thread"""
    print("🤖 Starting Bot...")
    from bot import main as bot_main
    asyncio.run(bot_main())

def run_web_server():
    """Simple HTTP server to keep Render alive"""
    from http.server import HTTPServer, BaseHTTPRequestHandler
    
    class HealthHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write(b"✅ Telegram Duplicate Monitor is running!")
    
    # Run on Render's assigned port
    port = int(os.environ.get("PORT", 8080))
    server = HTTPServer(('0.0.0.0', port), HealthHandler)
    print(f"🌐 Health check server running on port {port}")
    server.serve_forever()

def main():
    print("=" * 50)
    print("📱 Telegram Duplicate Monitor")
    print("=" * 50)
    
    # Import config to check if everything is set
    try:
        from config import API_ID, API_HASH, BOT_TOKEN
        print("✅ Config loaded successfully")
    except ImportError:
        print("❌ ERROR: Create config.py first!")
        print("Copy config.example.py to config.py and fill in your credentials")
        sys.exit(1)
    
    # Use threading instead of multiprocessing for async compatibility
    import threading
    
    # Start web server in background thread (required for Render)
    web_thread = threading.Thread(target=run_web_server, daemon=True)
    web_thread.start()
    print("✅ Health check server started")
    
    # Run monitor and bot in the same event loop
    print("\n🔄 Starting services...")
    
    # Create single event loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    # Import and run both in same loop
    async def run_all():
        from monitor import MessageMonitor
        from bot import start_bot
        
        monitor = MessageMonitor()
        
        # Start both tasks
        monitor_task = asyncio.create_task(monitor.start())
        bot_task = asyncio.create_task(start_bot())
        
        # Wait for both (they should run forever)
        await asyncio.gather(monitor_task, bot_task)
    
    try:
        loop.run_until_complete(run_all())
    except KeyboardInterrupt:
        print("\n👋 Shutting down...")
    finally:
        loop.close()

if __name__ == "__main__":
    main()
