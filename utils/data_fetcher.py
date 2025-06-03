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
    def __init__(self, config_path: str = '../Config/config.yaml', db_path: Optional[str] = None):
        """
        Initialize DataFetcher with configuration
        
        Args:
            config_path: Path to the config file containing Tushare token and other settings
            db_path: Path to the database file (optional, defaults to history.db)
        """
        # Use provided db_path or default to history.db
        if db_path is None:
            db_path = '../Database/history.db'
        
        self.db_manager = DatabaseManager(db_path=db_path)
        self.tushare_loader = TushareLoader(config_path, db_path=db_path)
    
    def _get_trading_days(self, start_date: str, end_date: str) -> pd.DatetimeIndex:
        """
        获取指定日期范围内的交易日
        
        Args:
            start_date: 开始日期，格式YYYYMMDD
            end_date: 结束日期，格式YYYYMMDD
            
        Returns:
            pd.DatetimeIndex: 交易日期索引
        """
        trading_days = self.tushare_loader.get_trading_calendar(start_date, end_date)
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
            print("❌ 数据为空")
            return False
            
        try:
            # 转换日期列为datetime类型
            df_dates = pd.DatetimeIndex(pd.to_datetime(df['trade_date']))
        except Exception as e:
            print(f"❌ 日期格式转换失败：{str(e)}")
            return False
        
        # 检查日期范围完整性
        if start_date and end_date:
            try:
                # 获取所有交易日
                trading_days = self._get_trading_days(start_date, end_date)
                
                # 检查是否有缺失的交易日
                missing_days = trading_days.difference(df_dates)
                if not missing_days.empty:
                    print(f"❌ 发现缺失的交易日期：")
                    for date in missing_days:
                        print(f"   - {date.strftime('%Y-%m-%d')}")
                    return False
            except Exception as e:
                print(f"❌ 交易日完整性检查失败：{str(e)}")
                return False
        
        # 检查数据质量
        required_columns = ['open', 'high', 'low', 'close', 'vol', 'amount', 'adj_factor']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            print(f"❌ 缺失必需的列：{', '.join(missing_columns)}")
            return False
            
        # 检查是否有空值
        try:
            null_columns = df[required_columns].columns[df[required_columns].isnull().any()].tolist()
            if null_columns:
                print(f"❌ 以下列存在空值：{', '.join(null_columns)}")
                print("空值详情：")
                for col in null_columns:
                    null_dates = df[df[col].isnull()]['trade_date'].tolist()
                    print(f"   - {col}列在以下日期存在空值：{', '.join(null_dates[:5])}{'...' if len(null_dates) > 5 else ''}")
                return False
        except Exception as e:
            print(f"❌ 空值检查失败：{str(e)}")
            return False
            
        print("✅ 数据完整性检查通过")
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
        try:
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
                    try:
                        is_complete = self._check_data_completeness(df, start_date, end_date)
                        if is_complete:
                            return df, f"✅ 从数据库获取完整数据成功：{ts_code}"
                    except Exception as check_error:
                        print(f"⚠️ 数据完整性检查失败：{str(check_error)}")
                    
                    # If data is incomplete, download missing data from Tushare
                    success, message, new_data = self.tushare_loader.download_and_store(
                        ts_code, 
                        start_date, 
                        end_date
                    )
                    
                    if success:
                        # Combine existing data with newly downloaded data
                        df = pd.concat([df, new_data]).drop_duplicates(subset=['trade_date', 'ts_code']).sort_values('trade_date')
                        return df, f"✅ 从Tushare补充缺失数据并获取完整数据成功：{ts_code}"
                    else:
                        return df, f"⚠️ 数据库数据不完整，且无法获取缺失数据：{message}"
            
            # If no data in database, download from Tushare
            success, message, new_data = self.tushare_loader.download_and_store(ts_code, start_date, end_date)
            
            if success:
                return new_data, f"✅ 从Tushare下载并获取数据成功：{ts_code}"
            else:
                return pd.DataFrame(), f"❌ 获取数据失败：{message}"
                
        except Exception as e:
            return pd.DataFrame(), f"❌ 获取股票数据时发生错误：{str(e)}"

# Example usage
if __name__ == "__main__":
    fetcher = DataFetcher()
    df, message = fetcher.get_stock_data("601318.SH", "20240101", "20240301")
    print(message)
    print(df.head()) 