# webserver.py
from flask import Flask, jsonify
import threading
import time
import logging
from database import DuplicateMonitorDB

logger = logging.getLogger("monitor-webserver")

app = Flask(__name__)
db = DuplicateMonitorDB()

START_TIME = time.time()

@app.route('/')
def home():
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Duplicate Monitor Status</title>
        <style>
            body {
                font-family: Arial, sans-serif;
                padding: 20px;
                background: #f5f5f5;
            }
            .container {
                max-width: 800px;
                margin: 0 auto;
                background: white;
                padding: 30px;
                border-radius: 10px;
                box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            }
            h1 {
                color: #333;
                border-bottom: 2px solid #4CAF50;
                padding-bottom: 10px;
            }
            .endpoint {
                background: #f9f9f9;
                padding: 15px;
                margin: 10px 0;
                border-left: 4px solid #4CAF50;
                border-radius: 4px;
            }
            code {
                background: #e9e9e9;
                padding: 2px 6px;
                border-radius: 3px;
                font-family: monospace;
            }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>📊 Duplicate Message Monitor</h1>
            <p>System is running. Use the following endpoints:</p>
            
            <div class="endpoint">
                <strong>GET <code>/health</code></strong>
                <p>Basic health check and uptime.</p>
            </div>
            
            <div class="endpoint">
                <strong>GET <code>/stats</code></strong>
                <p>System statistics and monitoring data.</p>
            </div>
            
            <div class="endpoint">
                <strong>GET <code>/status</code></strong>
                <p>Detailed system status and configuration.</p>
            </div>
            
            <p>System is designed to detect duplicate messages and allow manual replies.</p>
        </div>
    </body>
    </html>
    """

@app.route('/health')
def health():
    uptime = int(time.time() - START_TIME)
    return jsonify({
        'status': 'healthy',
        'uptime_seconds': uptime,
        'timestamp': time.time()
    })

@app.route('/stats')
def stats():
    try:
        stats_data = db.get_stats()
        
        # Get recent duplicates
        conn = db.get_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT d.id, d.chat_id, d.message_text, d.detected_at, d.status,
                   c.chat_name, c.user_id
            FROM duplicates d
            LEFT JOIN monitored_chats c ON d.chat_id = c.chat_id
            ORDER BY d.detected_at DESC
            LIMIT 10
        """)
        
        recent_duplicates = []
        for row in cur.fetchall():
            recent_duplicates.append({
                'id': row[0],
                'chat_id': row[1],
                'message_preview': row[2][:50] + '...' if len(row[2]) > 50 else row[2],
                'detected_at': row[3],
                'status': row[4],
                'chat_name': row[5],
                'user_id': row[6]
            })
        
        return jsonify({
            'system_stats': stats_data,
            'recent_duplicates': recent_duplicates,
            'active_monitors': len([c for c in stats_data.get('monitored_chats', [])])
        })
        
    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/status')
def status():
    uptime = int(time.time() - START_TIME)
    
    # Get database info
    conn = db.get_connection()
    cur = conn.cursor()
    
    table_counts = {}
    for table in ['monitoring_users', 'monitored_chats', 'messages', 'duplicates', 'bot_conversations']:
        try:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            table_counts[table] = cur.fetchone()[0]
        except:
            table_counts[table] = 'error'
    
    return jsonify({
        'status': 'running',
        'uptime_hours': round(uptime / 3600, 2),
        'start_time': time.ctime(START_TIME),
        'database': {
            'path': db.db_path,
            'tables': table_counts
        },
        'features': {
            'duplicate_detection': True,
            'manual_reply': True,
            'multiple_users': True,
            'web_monitoring': True
        }
    })

def run_server():
    """Run the Flask web server"""
    app.run(
        host='0.0.0.0',
        port=5000,
        debug=False,
        use_reloader=False,
        threaded=True
    )

def start_server_thread():
    """Start web server in a background thread"""
    server_thread = threading.Thread(target=run_server, daemon=True)
    server_thread.start()
    logger.info("Web server started on port 5000")
