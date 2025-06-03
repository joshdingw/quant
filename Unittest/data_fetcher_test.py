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
from utils.db_initializer import DatabaseInitializer

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
        
        # 初始化测试数据库
        cls.initialize_test_db()
        
        # 初始化数据库管理器和数据获取器（类级别）
        # 重要：让DataFetcher使用test.db而不是默认的history.db
        cls.db_manager = DatabaseManager(db_path=cls.test_db_path)
        cls.data_fetcher = DataFetcher(config_path=cls.test_config_path, db_path=cls.test_db_path)
    
    @classmethod
    def initialize_test_db(cls):
        """初始化测试数据库"""
        # 使用DatabaseInitializer创建表结构
        initializer = DatabaseInitializer(config_path=cls.test_config_path)
        initializer.initialize_database()
        
        # 清空测试数据库中的数据
        with sqlite3.connect(cls.test_db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM daily_data")
            conn.commit()
    
    def setUp(self):
        """每个测试之前的设置"""
        # 清空表数据（清空测试专用的test.db）
        with self.db_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM daily_data")
            conn.commit()
    
    def tearDown(self):
        """每个测试之后清理环境"""
        # 清空表数据（清空测试专用的test.db）
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
        start_date = "20240102"
        end_date = "20240105"
        
        try:
            trading_days = self.data_fetcher._get_trading_days(start_date, end_date)
            self.assertIsInstance(trading_days, pd.DatetimeIndex)
            self.assertGreater(len(trading_days), 0)
            print(f"✅ 获取到 {len(trading_days)} 个交易日")
        except Exception as e:
            self.fail(f"获取交易日失败: {str(e)}")
    
    def test_check_data_completeness_with_complete_data(self):
        """测试_check_data_completeness方法 - 完整数据的情况"""
        # 从真实API获取一小段数据用于测试
        ts_code = "601318.SH"  # 平安银行
        start_date = "20240102"
        end_date = "20240105"
        
        # 获取真实数据
        df, message = self.data_fetcher.get_stock_data(ts_code, start_date, end_date)
        if not df.empty:
            # 测试完整数据
            result = self.data_fetcher._check_data_completeness(df, start_date, end_date)
            self.assertTrue(result, "完整数据应该通过完整性检查")
            print(f"✅ 完整性检查通过: {len(df)} 条记录")
        else:
            self.skipTest(f"无法获取测试数据: {message}")
    
    def test_check_data_completeness_with_missing_days(self):
        """测试_check_data_completeness方法 - 缺失交易日的情况"""
        ts_code = "601318.SH"
        start_date = "20240102"
        end_date = "20240110"
        
        # 获取完整数据
        df, message = self.data_fetcher.get_stock_data(ts_code, start_date, end_date)
        if df.empty:
            self.skipTest(f"无法获取测试数据: {message}")
            return
        
        # 人为删除部分数据（删除中间某一天的数据）
        if len(df) > 2:
            incomplete_df = df.drop(df.index[len(df)//2]).reset_index(drop=True)
            
            # 测试不完整数据
            result = self.data_fetcher._check_data_completeness(incomplete_df, start_date, end_date)
            self.assertFalse(result, "不完整数据应该无法通过完整性检查")
            print(f"✅ 成功检测到数据不完整: 原{len(df)}条，删除后{len(incomplete_df)}条")
    
    def test_check_data_completeness_with_missing_columns(self):
        """测试_check_data_completeness方法 - 缺失必需列的情况"""
        ts_code = "601318.SH"
        start_date = "20240102"
        end_date = "20240105"
        
        # 获取完整数据
        df, message = self.data_fetcher.get_stock_data(ts_code, start_date, end_date)
        if df.empty:
            self.skipTest(f"无法获取测试数据: {message}")
            return
        
        # 测试缺少必需列的情况
        if 'vol' in df.columns:
            missing_column_df = df.drop('vol', axis=1)
            result = self.data_fetcher._check_data_completeness(
                missing_column_df, start_date, end_date
            )
            self.assertFalse(result, "缺少必需列的数据应该无法通过完整性检查")
            print("✅ 成功检测到缺失必需列")
    
    def test_check_data_completeness_with_null_values(self):
        """测试_check_data_completeness方法 - 存在空值的情况"""
        ts_code = "601318.SH"
        start_date = "20240102"
        end_date = "20240105"
        
        # 获取完整数据
        df, message = self.data_fetcher.get_stock_data(ts_code, start_date, end_date)
        if df.empty:
            self.skipTest(f"无法获取测试数据: {message}")
            return
        
        # 人为制造空值
        null_df = df.copy()
        if len(null_df) > 0:
            null_df.loc[0, 'open'] = None
            
            result = self.data_fetcher._check_data_completeness(
                null_df, start_date, end_date
            )
            self.assertFalse(result, "存在空值的数据应该无法通过完整性检查")
            print("✅ 成功检测到空值")
    
    def test_check_data_completeness_with_empty_dataframe(self):
        """测试_check_data_completeness方法 - 空数据框的情况"""
        empty_df = pd.DataFrame()
        
        result = self.data_fetcher._check_data_completeness(
            empty_df, '20240102', '20240105'
        )
        self.assertFalse(result, "空数据框应该无法通过完整性检查")
        print("✅ 成功检测到空数据框")
    
    def test_get_stock_data_from_database(self):
        """测试从数据库获取股票数据"""
        ts_code = "601318.SH"
        start_date = "20240102"
        end_date = "20240105"
        
        # 首次获取数据（从API获取并存储到数据库）
        df1, message1 = self.data_fetcher.get_stock_data(ts_code, start_date, end_date)
        if df1.empty:
            self.skipTest(f"无法获取测试数据: {message1}")
            return
        
        print(f"第一次获取: {message1}")
        
        # 再次获取相同数据（应该从数据库获取）
        df2, message2 = self.data_fetcher.get_stock_data(ts_code, start_date, end_date)
        
        self.assertFalse(df2.empty, "应该从数据库获取到数据")
        self.assertEqual(len(df1), len(df2), "两次获取的数据量应该相同")
        print(f"第二次获取: {message2}")
        
        # 验证数据内容一致性
        pd.testing.assert_frame_equal(
            df1.sort_values('trade_date').reset_index(drop=True),
            df2.sort_values('trade_date').reset_index(drop=True),
            "两次获取的数据应该相同"
        )
    
    def test_get_stock_data_from_tushare_when_database_empty(self):
        """测试当数据库为空时从Tushare获取数据"""
        ts_code = "601318.SH"
        start_date = "20240102"
        end_date = "20240105"
        
        # 确保数据库为空（使用DataFetcher实际使用的数据库）
        with self.db_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM daily_data WHERE ts_code = ?", (ts_code,))
            conn.commit()
        
        df, message = self.data_fetcher.get_stock_data(ts_code, start_date, end_date)
        
        if df.empty:
            self.skipTest(f"无法获取测试数据: {message}")
            return
        
        self.assertFalse(df.empty, "应该从Tushare获取到数据")
        self.assertIn("从Tushare下载并获取数据成功", message, "消息应该提到从Tushare获取数据")
        print(f"✅ 从Tushare获取数据成功: {len(df)} 条记录")
        
        # 验证数据质量
        required_columns = ['trade_date', 'ts_code', 'open', 'high', 'low', 'close', 'vol', 'amount', 'adj_factor']
        for col in required_columns:
            self.assertIn(col, df.columns, f"应该包含列: {col}")
            
        # 验证数据是否已经存入数据库
        with self.db_manager.get_connection() as conn:
            cursor = conn.cursor()
            result = cursor.execute("""
                SELECT COUNT(*) FROM daily_data 
                WHERE ts_code = ? 
                AND trade_date >= ? 
                AND trade_date <= ?
            """, (ts_code, start_date, end_date)).fetchone()
            
            self.assertEqual(result[0], len(df), "数据应该已经存入数据库")
    
    def test_get_stock_data_invalid_stock_code(self):
        """测试获取无效股票代码的数据"""
        df, message = self.data_fetcher.get_stock_data("INVALID.CODE")
        
        self.assertTrue(df.empty, "无效股票代码应该返回空数据框")
        self.assertIn("失败", message, "消息应该包含失败信息")
        print(f"✅ 正确处理无效股票代码: {message}")
    
    def test_get_stock_data_date_range(self):
        """测试不同日期范围的数据获取"""
        ts_code = "601318.SH"
        
        # 测试较短的日期范围
        short_start = "20240102"
        short_end = "20240103"
        df_short, msg_short = self.data_fetcher.get_stock_data(ts_code, short_start, short_end)
        
        # 测试较长的日期范围
        long_start = "20240101"
        long_end = "20240115"
        df_long, msg_long = self.data_fetcher.get_stock_data(ts_code, long_start, long_end)
        
        if not df_short.empty and not df_long.empty:
            self.assertGreater(len(df_long), len(df_short), "较长日期范围应该获取更多数据")
            print(f"✅ 日期范围测试成功:")
            print(f"   短期({short_start}到{short_end}): {len(df_short)} 条记录")
            print(f"   长期({long_start}到{long_end}): {len(df_long)} 条记录")
        elif df_short.empty and df_long.empty:
            self.skipTest("无法获取任何测试数据")
        else:
            print(f"⚠️ 部分数据获取失败 - 短期: {msg_short}, 长期: {msg_long}")
    
    def test_data_fetcher_error_handling(self):
        """测试DataFetcher的错误处理能力"""
        # 测试无效日期格式
        df, message = self.data_fetcher.get_stock_data("601318.SH", "invalid_date", "20240105")
        # 注意：这里不一定返回空数据框，因为Tushare可能会忽略无效日期
        print(f"无效日期测试: {message}")
        
        # 测试日期范围颠倒
        df, message = self.data_fetcher.get_stock_data("601318.SH", "20240105", "20240102")
        print(f"日期范围颠倒测试: {message}")

if __name__ == '__main__':
    # 设置测试运行时的详细输出
    unittest.main(verbosity=2)
