import sqlite3
import os
from typing import Optional

class DatabaseManager:
    def __init__(self, db_path: str = '../Database/history.db'):
        """
        初始化数据库管理器
        
        Args:
            db_path: SQLite数据库文件路径
        """
        self.db_path = db_path
    
    def get_connection(self) -> sqlite3.Connection:
        """获取数据库连接"""
        return sqlite3.connect(self.db_path) 