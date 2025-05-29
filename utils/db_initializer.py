import sqlite3
import os
import yaml
from typing import Optional

class DatabaseInitializer:
    def __init__(self, config_path: str = '../Config/config.yaml'):
        """
        初始化数据库初始化器
        
        Args:
            config_path: 配置文件路径
        """
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.history_db_path = self.config.get('database_path', '../Database/history.db')
        self.test_db_path = '../Database/test.db'
    
    def initialize_database(self):
        """
        初始化所有数据库和表结构
        """
        # 初始化历史数据库
        self._initialize_single_database(self.history_db_path)
        # 初始化测试数据库
        self._initialize_single_database(self.test_db_path)
    
    def _initialize_single_database(self, db_path: str):
        """
        初始化单个数据库
        
        Args:
            db_path: 数据库文件路径
        """
        # 确保数据库目录存在
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        # 创建数据库连接
        with sqlite3.connect(db_path) as conn:
            # 创建daily_data表
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
                adj_factor REAL,
                PRIMARY KEY (trade_date, ts_code)
            );
            """
            conn.execute(create_table_sql)
            conn.commit()
            
            print(f"✅ 数据库 {os.path.basename(db_path)} 初始化完成")

if __name__ == "__main__":
    # 当直接运行此文件时，执行所有数据库初始化
    initializer = DatabaseInitializer()
    initializer.initialize_database() 