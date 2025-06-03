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

    def _check_moneyflow_data_completeness(self, df: pd.DataFrame, start_date: Optional[str], 
                                         end_date: Optional[str]) -> bool:
        """
        检查资金流数据的完整性
        
        Args:
            df: DataFrame containing moneyflow data
            start_date: Start date in YYYYMMDD format
            end_date: End date in YYYYMMDD format
            
        Returns:
            bool: True if data is complete, False otherwise
        """
        if df.empty:
            print("❌ 资金流数据为空")
            return False
            
        try:
            # 转换日期列为datetime类型
            df_dates = pd.DatetimeIndex(pd.to_datetime(df['trade_date']))
        except Exception as e:
            print(f"❌ 资金流数据日期格式转换失败：{str(e)}")
            return False
        
        # 检查日期范围完整性
        if start_date and end_date:
            try:
                # 获取所有交易日
                trading_days = self._get_trading_days(start_date, end_date)
                
                # 检查是否有缺失的交易日
                missing_days = trading_days.difference(df_dates.unique())
                if not missing_days.empty:
                    print(f"❌ 资金流数据发现缺失的交易日期：")
                    for date in missing_days[:5]:  # 只显示前5个缺失日期
                        print(f"   - {date.strftime('%Y-%m-%d')}")
                    if len(missing_days) > 5:
                        print(f"   - ...等{len(missing_days)}个缺失日期")
                    return False
            except Exception as e:
                print(f"❌ 资金流数据交易日完整性检查失败：{str(e)}")
                return False
        
        # 检查数据质量
        required_columns = ['ts_code', 'trade_date', 'buy_elg_amount', 'buy_elg_vol']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            print(f"❌ 资金流数据缺失必需的列：{', '.join(missing_columns)}")
            return False
            
        print("✅ 资金流数据完整性检查通过")
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
                            return df, f"✅ 从数据库获取完整股票数据成功：{ts_code}"
                    except Exception as check_error:
                        print(f"⚠️ 数据完整性检查失败：{str(check_error)}")
                    
                    # If data is incomplete, download missing data from Tushare
                    success, message, new_data = self.tushare_loader.download_and_store(
                        ts_code, start_date, end_date
                    )
                    
                    if success:
                        # Combine existing data with newly downloaded data
                        df = pd.concat([df, new_data]).drop_duplicates(subset=['trade_date', 'ts_code']).sort_values('trade_date')
                        return df, f"✅ 从Tushare补充缺失股票数据并获取完整数据成功：{ts_code}"
                    else:
                        return df, f"⚠️ 数据库股票数据不完整，且无法获取缺失数据：{message}"
            
            # If no data in database, download from Tushare
            success, message, new_data = self.tushare_loader.download_and_store(ts_code, start_date, end_date)
            
            if success:
                return new_data, f"✅ 从Tushare下载并获取股票数据成功：{ts_code}"
            else:
                return pd.DataFrame(), f"❌ 获取股票数据失败：{message}"
                
        except Exception as e:
            return pd.DataFrame(), f"❌ 获取股票数据时发生错误：{str(e)}"

    def get_index_data(self, ts_code: str, start_date: Optional[str] = None, 
                      end_date: Optional[str] = None) -> Tuple[pd.DataFrame, str]:
        """
        Get index data, first from database, then from Tushare if needed
        
        Args:
            ts_code: Index code (e.g., '000300.SH')
            start_date: Start date in YYYYMMDD format (optional)
            end_date: End date in YYYYMMDD format (optional)
            
        Returns:
            Tuple[pd.DataFrame, str]: (DataFrame containing index data, message)
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
                            return df, f"✅ 从数据库获取完整指数数据成功：{ts_code}"
                    except Exception as check_error:
                        print(f"⚠️ 数据完整性检查失败：{str(check_error)}")
                    
                    # If data is incomplete, download missing data from Tushare
                    success, message, new_data = self.tushare_loader.download_index_data(
                        ts_code, start_date, end_date
                    )
                    
                    if success:
                        # Combine existing data with newly downloaded data
                        df = pd.concat([df, new_data]).drop_duplicates(subset=['trade_date', 'ts_code']).sort_values('trade_date')
                        return df, f"✅ 从Tushare补充缺失指数数据并获取完整数据成功：{ts_code}"
                    else:
                        return df, f"⚠️ 数据库指数数据不完整，且无法获取缺失数据：{message}"
            
            # If no data in database, download from Tushare
            success, message, new_data = self.tushare_loader.download_index_data(ts_code, start_date, end_date)
            
            if success:
                return new_data, f"✅ 从Tushare下载并获取指数数据成功：{ts_code}"
            else:
                return pd.DataFrame(), f"❌ 获取指数数据失败：{message}"
                
        except Exception as e:
            return pd.DataFrame(), f"❌ 获取指数数据时发生错误：{str(e)}"

    def get_moneyflow_data(self, start_date: Optional[str] = None, 
                          end_date: Optional[str] = None) -> Tuple[pd.DataFrame, str]:
        """
        获取资金流数据，优先从数据库获取，如需要则从Tushare补充
        
        Args:
            start_date: 开始日期，格式：YYYYMMDD (optional)
            end_date: 结束日期，格式：YYYYMMDD (optional)
            
        Returns:
            Tuple[pd.DataFrame, str]: (DataFrame containing moneyflow data, message)
        """
        try:
            # 首先尝试从数据库获取数据
            with self.db_manager.get_connection() as conn:
                query = """
                SELECT * FROM moneyflow_data
                """
                params = []
                
                if start_date:
                    query += " WHERE trade_date >= ?"
                    params.append(start_date)
                    
                    if end_date:
                        query += " AND trade_date <= ?"
                        params.append(end_date)
                elif end_date:
                    query += " WHERE trade_date <= ?"
                    params.append(end_date)
                    
                query += " ORDER BY trade_date, ts_code"
                
                df = pd.read_sql_query(query, conn, params=params)
                
                if not df.empty:
                    # 检查数据是否完整
                    try:
                        is_complete = self._check_moneyflow_data_completeness(df, start_date, end_date)
                        if is_complete:
                            return df, f"✅ 从数据库获取完整资金流数据成功：{len(df)} 条记录"
                    except Exception as check_error:
                        print(f"⚠️ 资金流数据完整性检查失败：{str(check_error)}")
                    
                    # 如果数据不完整，从Tushare下载缺失数据
                    success, message, new_data = self.tushare_loader.download_moneyflow_data(
                        start_date, end_date
                    )
                    
                    if success:
                        # 重新查询数据库获取更新后的完整数据
                        updated_df = pd.read_sql_query(query, conn, params=params)
                        return updated_df, f"✅ 从Tushare补充缺失资金流数据并获取完整数据成功：{message}"
                    else:
                        return df, f"⚠️ 数据库资金流数据不完整，且无法获取缺失数据：{message}"
            
            # 如果数据库中没有数据，从Tushare下载
            success, message, new_data = self.tushare_loader.download_moneyflow_data(start_date, end_date)
            
            if success:
                return new_data, f"✅ 从Tushare下载并获取资金流数据成功：{message}"
            else:
                return pd.DataFrame(), f"❌ 获取资金流数据失败：{message}"
                
        except Exception as e:
            return pd.DataFrame(), f"❌ 获取资金流数据时发生错误：{str(e)}"

    def get_all_stock_codes(self) -> Tuple[pd.DataFrame, str]:
        """
        获取所有A股股票代码列表（排除ST股票和北交所股票）
        
        Returns:
            Tuple[pd.DataFrame, str]: (DataFrame containing stock codes, message)
        """
        try:
            # 获取在市、退市、暂停上市的股票列表
            success, message, stock_list = self.tushare_loader.get_stock_basic_info()
            
            if success:
                return stock_list, f"✅ 获取股票代码列表成功：{len(stock_list)} 只股票"
            else:
                return pd.DataFrame(), f"❌ 获取股票代码列表失败：{message}"
                
        except Exception as e:
            return pd.DataFrame(), f"❌ 获取股票代码列表时发生错误：{str(e)}"

    def get_daily_basic_data(self, trade_date: str, ts_code: Optional[str] = None) -> Tuple[pd.DataFrame, str]:
        """
        获取指定交易日的股票基本信息（包含流通市值等）
        
        Args:
            trade_date: 交易日期，格式：YYYYMMDD
            ts_code: 股票代码（可选），如果不指定则获取所有股票
            
        Returns:
            Tuple[pd.DataFrame, str]: (DataFrame containing daily basic data, message)
        """
        try:
            # 从Tushare获取每日基本信息
            success, message, daily_basic_data = self.tushare_loader.get_daily_basic_data(trade_date, ts_code)
            
            if success:
                return daily_basic_data, f"✅ 获取{trade_date}基本信息数据成功：{len(daily_basic_data)} 条记录"
            else:
                return pd.DataFrame(), f"❌ 获取{trade_date}基本信息数据失败：{message}"
                
        except Exception as e:
            return pd.DataFrame(), f"❌ 获取基本信息数据时发生错误：{str(e)}"

    def get_batch_stock_data(self, stock_codes: list, start_date: Optional[str] = None, 
                            end_date: Optional[str] = None, max_workers: int = 5) -> Tuple[pd.DataFrame, str]:
        """
        批量获取多只股票的数据
        
        Args:
            stock_codes: 股票代码列表
            start_date: 开始日期，格式YYYYMMDD（可选）
            end_date: 结束日期，格式YYYYMMDD（可选）
            max_workers: 最大并发数
            
        Returns:
            Tuple[pd.DataFrame, str]: (DataFrame containing all stock data, message)
        """
        try:
            from concurrent.futures import ThreadPoolExecutor, as_completed
            import time
            
            all_data = []
            success_count = 0
            failed_stocks = []
            
            print(f"开始批量获取{len(stock_codes)}只股票的数据...")
            
            def get_single_stock(ts_code):
                try:
                    df, msg = self.get_stock_data(ts_code, start_date, end_date)
                    if not df.empty:
                        return True, ts_code, df, msg
                    else:
                        return False, ts_code, pd.DataFrame(), msg
                except Exception as e:
                    return False, ts_code, pd.DataFrame(), str(e)
            
            # 使用线程池并发获取数据
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 提交所有任务
                futures = {executor.submit(get_single_stock, code): code for code in stock_codes}
                
                # 处理完成的任务
                for i, future in enumerate(as_completed(futures)):
                    ts_code = futures[future]
                    try:
                        success, code, data, msg = future.result()
                        if success:
                            all_data.append(data)
                            success_count += 1
                            print(f"✅ [{i+1}/{len(stock_codes)}] {code}: {msg}")
                        else:
                            failed_stocks.append(code)
                            print(f"❌ [{i+1}/{len(stock_codes)}] {code}: {msg}")
                    except Exception as e:
                        failed_stocks.append(ts_code)
                        print(f"❌ [{i+1}/{len(stock_codes)}] {ts_code}: 处理异常 - {str(e)}")
                    
                    # 控制请求频率，避免API限制
                    if i % 10 == 0 and i > 0:
                        time.sleep(1)
            
            # 合并所有数据
            if all_data:
                combined_data = pd.concat(all_data, ignore_index=True)
                combined_data = combined_data.sort_values(['ts_code', 'trade_date'])
                
                message = f"✅ 批量获取股票数据完成：成功{success_count}只，失败{len(failed_stocks)}只，共{len(combined_data)}条记录"
                if failed_stocks:
                    message += f"\n失败股票：{', '.join(failed_stocks[:10])}{'...' if len(failed_stocks) > 10 else ''}"
                
                return combined_data, message
            else:
                return pd.DataFrame(), f"❌ 批量获取股票数据失败：所有{len(stock_codes)}只股票都获取失败"
                
        except Exception as e:
            return pd.DataFrame(), f"❌ 批量获取股票数据时发生错误：{str(e)}"

# Example usage
if __name__ == "__main__":
    fetcher = DataFetcher()
    
    # 获取股票数据
    stock_df, stock_message = fetcher.get_stock_data("601318.SH", "20240101", "20240301")
    print("股票数据:", stock_message)
    print(stock_df.head())
    
    # 获取指数数据
    index_df, index_message = fetcher.get_index_data("000300.SH", "20240101", "20240301")
    print("指数数据:", index_message)
    print(index_df.head())
    
    # 获取资金流数据
    moneyflow_df, moneyflow_message = fetcher.get_moneyflow_data("20240101", "20240201")
    print("资金流数据:", moneyflow_message)
    print(moneyflow_df.head()) 