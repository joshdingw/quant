import pandas as pd
import numpy as np
from typing import Optional, Tuple
import os
import sys

# Add parent directory to path if running directly
if __name__ == "__main__":
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from utils.db_manager import DatabaseManager
    from utils.tushare_loader import TushareLoader
else:
    from .db_manager import DatabaseManager
    from .tushare_loader import TushareLoader

class DataFetcher:
    def __init__(self, config_path: str = '../Config/config.yaml'):
        """
        Initialize DataFetcher with configuration
        
        Args:
            config_path: Path to the config file containing Tushare token and other settings
        """
        self.db_manager = DatabaseManager()
        self.tushare_loader = TushareLoader(config_path)
    
    def _get_trading_days(self, start_date: str, end_date: str) -> pd.DatetimeIndex:
        """
        Get all trading days between start_date and end_date from Tushare
        
        Args:
            start_date: Start date in YYYYMMDD format
            end_date: End date in YYYYMMDD format
            
        Returns:
            pd.DatetimeIndex: Index of trading days
        """
        # 从Tushare获取交易日历
        trading_days = self.tushare_loader.get_trading_calendar(start_date, end_date)
        if trading_days is None or trading_days.empty:
            raise ValueError(f"无法从Tushare获取交易日历: {start_date} 到 {end_date}")
        return pd.DatetimeIndex(pd.to_datetime(trading_days['cal_date']))
    
    def _check_data_completeness(self, df: pd.DataFrame, start_date: Optional[str], 
                               end_date: Optional[str]) -> bool:
        """
        Check if the data in DataFrame is complete for the given date range
        
        Args:
            df: DataFrame containing stock data
            start_date: Start date in YYYYMMDD format
            end_date: End date in YYYYMMDD format
            
        Returns:
            bool: True if data is complete, False otherwise
        """
        if df.empty:
            return False
            
        # 转换日期列为datetime类型
        df_dates = pd.DatetimeIndex(pd.to_datetime(df['trade_date']))
        
        # 检查日期范围完整性
        if start_date and end_date:
            # 获取所有交易日
            trading_days = self._get_trading_days(start_date, end_date)
            
            # 检查是否有缺失的交易日
            missing_days = trading_days.difference(df_dates)
            if not missing_days.empty:
                return False
        
        # 检查数据质量
        required_columns = ['open', 'high', 'low', 'close', 'vol', 'amount']
        if not all(col in df.columns for col in required_columns):
            return False
            
        # 检查是否有空值
        if df[required_columns].isnull().any().any():
            return False
            
        return True
    
    def get_stock_data(self, ts_code: str, start_date: Optional[str] = None, 
                      end_date: Optional[str] = None) -> Tuple[pd.DataFrame, str]:
        """
        Get stock data, first from database, then from Tushare if needed
        
        Args:
            ts_code: Stock code (e.g., '601318.SH')
            start_date: Start date in YYYYMMDD format (optional)
            end_date: End date in YYYYMMDD format (optional)
            
        Returns:
            Tuple[pd.DataFrame, str]: (DataFrame containing stock data, message)
        """
        # First try to get data from database
        with self.db_manager.get_connection() as conn:
            query = """
            SELECT * FROM daily_data 
            WHERE ts_code = ?
            """
            params = [ts_code]
            
            if start_date:
                query += " AND trade_date >= ?"
                params.append(start_date)
            if end_date:
                query += " AND trade_date <= ?"
                params.append(end_date)
                
            query += " ORDER BY trade_date"
            
            df = pd.read_sql_query(query, conn, params=params)
            
            if not df.empty:
                # Check if data is complete
                is_complete = self._check_data_completeness(df, start_date, end_date)
                if is_complete:
                    return df, f"✅ 从数据库获取完整数据成功：{ts_code}"
                
                # If data is incomplete, download missing data from Tushare
                success, message, new_data = self.tushare_loader.download_and_store(
                    ts_code, 
                    start_date, 
                    end_date
                )
                
                if success:
                    # Combine existing data with newly downloaded data
                    df = pd.concat([df, new_data]).drop_duplicates(subset=['trade_date', 'ts_code']).sort_values('trade_date')
                    return df, f"✅ 补充缺失数据并获取完整数据成功：{ts_code}"
                else:
                    return df, f"⚠️ 数据库数据不完整，且无法从Tushare获取缺失数据：{message}"
        
        # If no data in database, download from Tushare
        success, message, new_data = self.tushare_loader.download_and_store(ts_code, start_date, end_date)
        
        if success:
            return new_data, f"✅ 从Tushare下载并获取数据成功：{ts_code}"
        else:
            return pd.DataFrame(), f"❌ 获取数据失败：{message}"

# Example usage
if __name__ == "__main__":
    fetcher = DataFetcher()
    df, message = fetcher.get_stock_data("601318.SH", "20230101", "20240301")
    print(message)
    print(df.head()) 