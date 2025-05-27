import unittest
import os
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
import sys
import shutil
import time

# 添加父目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.data_fetcher import DataFetcher
from utils.db_manager import DatabaseManager

class TestDataFetcher(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """在所有测试之前设置测试环境"""
        # 创建测试数据库路径
        cls.test_db_path = os.path.join('../Database', 'test.db')
        
        # 确保数据库目录存在
        os.makedirs(os.path.dirname(cls.test_db_path), exist_ok=True)
        
        # 创建测试配置文件路径
        cls.test_config_path = os.path.join('../Config', 'config.yaml')
        
        # 检查配置目录和文件是否存在，不存在则报错
        if not os.path.exists(os.path.dirname(cls.test_config_path)):
            raise FileNotFoundError(f"配置目录不存在: {os.path.dirname(cls.test_config_path)}")
        if not os.path.exists(cls.test_config_path):
            raise FileNotFoundError(f"测试配置文件不存在: {cls.test_config_path}")
        
        # 初始化测试数据库（只在类初始化时创建一次）
        cls.initialize_test_db()
        
        # 初始化数据库管理器和数据获取器（类级别）
        cls.db_manager = DatabaseManager(db_path=cls.test_db_path)
        cls.data_fetcher = DataFetcher(config_path=cls.test_config_path)
    
    @classmethod
    def initialize_test_db(cls):
        """使用与history.db相同的结构初始化测试数据库"""
        # 创建新的数据库连接
        conn = sqlite3.connect(cls.test_db_path)
        cursor = conn.cursor()
        
        # 使用与history.db相同的结构创建daily_data表
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
        cursor.execute(create_table_sql)
        
        # 清空表数据
        cursor.execute("DELETE FROM daily_data")
        
        conn.commit()
        conn.close()
    
    def tearDown(self):
        """每个测试之后清理环境"""
        # 清空表数据
        with self.db_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM daily_data")
            conn.commit()
    
    @classmethod
    def tearDownClass(cls):
        """所有测试之后清理测试环境"""
        # 关闭数据库连接
        if hasattr(cls, 'db_manager'):
            del cls.db_manager
        if hasattr(cls, 'data_fetcher'):
            del cls.data_fetcher
    
    def test_init(self):
        """测试DataFetcher的初始化"""
        self.assertIsNotNone(self.data_fetcher)
        self.assertIsNotNone(self.data_fetcher.db_manager)
        self.assertIsNotNone(self.data_fetcher.tushare_loader)
    
    def test_get_trading_days(self):
        """测试_get_trading_days方法"""
        # 测试有效日期范围
        start_date = "20240101"
        end_date = "20240110"
        trading_days = self.data_fetcher._get_trading_days(start_date, end_date)
        self.assertIsInstance(trading_days, pd.DatetimeIndex)
        self.assertGreater(len(trading_days), 0)
        
        # 测试无效日期范围
        with self.assertRaises(ValueError):
            self.data_fetcher._get_trading_days("20240101", "20230101")
    
    def test_check_data_completeness_missing_day(self):
        """测试_check_data_completeness方法 - 缺失交易日的情况"""
        # 从Tushare获取真实数据
        ts_code = "601318.SH"  # 平安银行
        start_date = "20240101"
        end_date = "20240110"
        
        # 获取完整数据
        df, message = self.data_fetcher.get_stock_data(ts_code, start_date, end_date)
        self.assertFalse(df.empty, "应该成功获取到数据")
        
        # 将完整数据存入数据库
        with self.db_manager.get_connection() as conn:
            df.to_sql('daily_data', conn, if_exists='append', index=False)
        
        # 测试完整数据
        self.assertTrue(self.data_fetcher._check_data_completeness(
            df, start_date, end_date), "完整数据应该通过完整性检查")
        
        # 人为删除部分数据（删除中间某一天的数据）
        with self.db_manager.get_connection() as conn:
            cursor = conn.cursor()
            # 删除中间某一天的数据
            middle_date = df['trade_date'].iloc[len(df)//2]
            cursor.execute("DELETE FROM daily_data WHERE trade_date = ? AND ts_code = ?", 
                         (middle_date, ts_code))
            conn.commit()
        
        # 从数据库重新读取数据
        with self.db_manager.get_connection() as conn:
            incomplete_df = pd.read_sql_query(
                "SELECT * FROM daily_data WHERE ts_code = ? ORDER BY trade_date",
                conn, params=[ts_code]
            )
        
        # 测试不完整数据
        self.assertFalse(self.data_fetcher._check_data_completeness(
            incomplete_df, start_date, end_date), "不完整数据应该无法通过完整性检查")
    
    def test_check_data_completeness_missing_column(self):
        """测试_check_data_completeness方法 - 缺失必需列的情况"""
        # 从Tushare获取真实数据
        ts_code = "601318.SH"  # 平安银行
        start_date = "20240101"
        end_date = "20240110"
        
        # 获取完整数据
        df, message = self.data_fetcher.get_stock_data(ts_code, start_date, end_date)
        self.assertFalse(df.empty, "应该成功获取到数据")
        
        # 测试缺少必需列的情况
        missing_column_df = df.copy()  # 使用完整数据的副本
        missing_column_df = missing_column_df.drop('vol', axis=1)
        self.assertFalse(self.data_fetcher._check_data_completeness(
            missing_column_df, start_date, end_date), "缺少必需列的数据应该无法通过完整性检查")
        
        # 测试空数据框
        self.assertFalse(self.data_fetcher._check_data_completeness(
            pd.DataFrame(), start_date, end_date), "空数据框应该无法通过完整性检查")
    
    def test_get_stock_data(self):
        """测试get_stock_data方法"""
        # 测试无效股票代码
        df, message = self.data_fetcher.get_stock_data("INVALID.CODE")
        self.assertTrue(df.empty)
        self.assertIn("获取数据失败", message)
        
        # 注意：测试真实股票代码需要模拟Tushare API调用
        # 这将在单独的测试中实现，使用适当的模拟方法

if __name__ == '__main__':
    unittest.main()
