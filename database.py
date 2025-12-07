# database.py
import sqlite3
import hashlib
from datetime import datetime

class Database:
    def __init__(self, db_name="messages.db"):
        self.conn = sqlite3.connect(db_name)
        self.create_tables()
    
    def create_tables(self):
        cursor = self.conn.cursor()
        # Messages table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER,
                message_id INTEGER,
                sender_id INTEGER,
                content_hash TEXT,
                timestamp DATETIME,
                UNIQUE(chat_id, message_id)
            )
        ''')
        # Alerts table (for pending replies)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER,
                message_id INTEGER,
                message_text TEXT,
                alert_sent BOOLEAN DEFAULT 0,
                replied BOOLEAN DEFAULT 0,
                reply_text TEXT,
                created_at DATETIME
            )
        ''')
        self.conn.commit()
    
    def create_hash(self, text):
        """Create hash for message content"""
        if not text:
            text = ""
        normalized = " ".join(text.lower().strip().split())
        return hashlib.md5(normalized.encode()).hexdigest()[:12]
    
    def is_duplicate(self, chat_id, content_hash):
        """Check if message is duplicate in same chat"""
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT 1 FROM messages WHERE chat_id=? AND content_hash=? LIMIT 1",
            (chat_id, content_hash)
        )
        return cursor.fetchone() is not None
    
    def save_message(self, chat_id, message_id, sender_id, content):
        """Save new message to database"""
        content_hash = self.create_hash(content)
        cursor = self.conn.cursor()
        cursor.execute(
            '''INSERT INTO messages 
               (chat_id, message_id, sender_id, content_hash, timestamp) 
               VALUES (?, ?, ?, ?, ?)''',
            (chat_id, message_id, sender_id, content_hash, datetime.now())
        )
        self.conn.commit()
        return content_hash
    
    def save_alert(self, chat_id, message_id, message_text):
        """Save duplicate alert for manual reply"""
        cursor = self.conn.cursor()
        cursor.execute(
            '''INSERT INTO alerts 
               (chat_id, message_id, message_text, created_at) 
               VALUES (?, ?, ?, ?)''',
            (chat_id, message_id, message_text, datetime.now())
        )
        self.conn.commit()
        return cursor.lastrowid  # Return alert ID
    
    def get_pending_alerts(self):
        """Get alerts waiting for reply"""
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT id, chat_id, message_id, message_text FROM alerts WHERE replied=0"
        )
        return cursor.fetchall()
    
    def mark_replied(self, alert_id, reply_text):
        """Mark alert as replied"""
        cursor = self.conn.cursor()
        cursor.execute(
            "UPDATE alerts SET replied=1, reply_text=? WHERE id=?",
            (reply_text, alert_id)
        )
        self.conn.commit()
        
# Add at the end of database.py
def init_db():
    """Initialize database on first run"""
    db = Database()
    print("✅ Database initialized")
    return db

if __name__ == "__main__":
    init_db()
