import sqlite3
import os
from typing import Optional

class DatabaseManager:
    def __init__(self, db_path: str = '../Database/history.db'):
        """
        Initialize database manager
        
        Args:
            db_path: Path to the SQLite database file
        """
        self.db_path = db_path
        self._ensure_db_directory()
        self._init_tables()
    
    def _ensure_db_directory(self):
        """Ensure database directory exists"""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
    
    def _init_tables(self):
        """Initialize database tables"""
        with self.get_connection() as conn:
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS daily_data (
                trade_date TEXT NOT NULL,
                ts_code TEXT NOT NULL,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                vol REAL,
                amount REAL,
                PRIMARY KEY (trade_date, ts_code)
            );
            """
            conn.execute(create_table_sql)
            conn.commit()
    
    def get_connection(self) -> sqlite3.Connection:
        """Get a new database connection"""
        return sqlite3.connect(self.db_path) 