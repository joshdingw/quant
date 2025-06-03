import tushare as ts
import pandas as pd
import yaml
from typing import Optional, Tuple
import os
import sys

# Add parent directory to path if running directly
if __name__ == "__main__":
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from utils.db_manager import DatabaseManager
else:
    from .db_manager import DatabaseManager

class TushareLoader:
    def __init__(self, config_path: str = '../Config/config.yaml', db_path: Optional[str] = None):
        """
        Initialize TushareLoader with configuration
        
        Args:
            config_path: Path to the config file containing Tushare token and other settings
            db_path: Path to the database file (optional, defaults to history.db)
        """
        # Load configuration
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        # Initialize Tushare
        ts.set_token(self.config['tushare_token'])
        
        # Set default date range
        self.start_date = self.config.get('start_date', '20240101')
        self.end_date = self.config.get('end_date', '20240501')
        
        # Initialize database manager with specified or default path
        if db_path is None:
            db_path = '../Database/history.db'
        self.db_manager = DatabaseManager(db_path=db_path)
    
    def get_trading_calendar(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        从Tushare获取交易日历
        
        Args:
            start_date: 开始日期，格式：YYYYMMDD
            end_date: 结束日期，格式：YYYYMMDD
            
        Returns:
            pd.DataFrame: 包含交易日历的DataFrame，包含所有必要的列（cal_date, is_open等）
        """
        try:
            # 调用Tushare的交易日历接口
            df = ts.pro_api().trade_cal(exchange='SSE', start_date=start_date, end_date=end_date)
            
            # 检查数据有效性
            if df is None or df.empty:
                raise ValueError(f"无法获取交易日历: {start_date} 到 {end_date}")
            
            # 检查必要的列是否存在
            required_columns = ['cal_date', 'is_open']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                raise ValueError(f"交易日历数据缺少必要的列: {', '.join(missing_columns)}")
            
            # 只保留交易日（is_open=1）
            df = df[df['is_open'] == 1]
            
            return df
            
        except Exception as e:
            raise ValueError(f"获取交易日历失败: {str(e)}")
    
    def download_and_store(self, ts_code: str, start_date: Optional[str] = None, 
                          end_date: Optional[str] = None) -> Tuple[bool, str, pd.DataFrame]:
        """
        Download and store stock data for a given ts_code
        
        Args:
            ts_code: Stock code (e.g., '601318.SH')
            start_date: Start date in YYYYMMDD format (optional)
            end_date: End date in YYYYMMDD format (optional)
            
        Returns:
            Tuple[bool, str, pd.DataFrame]: (success status, message, downloaded data)
        """
        try:
            # Use provided dates or defaults
            start = start_date or self.start_date
            end = end_date or self.end_date
            
            # Download data
            try:
                df = ts.pro_bar(ts_code=ts_code, start_date=start, end_date=end, adj=None)
                if df is None:
                    return False, f"❌ API返回空数据: {ts_code}", pd.DataFrame()
                if df.empty:
                    return False, f"❌ 指定日期范围内没有数据: {ts_code} ({start} 到 {end})", pd.DataFrame()
            except Exception as api_error:
                return False, f"❌ API调用失败: {ts_code} - {str(api_error)}", pd.DataFrame()
            
            # 获取复权因子
            try:
                adj_factors = self.get_adj_factor(ts_code, start, end)
                if adj_factors.empty:
                    return False, f"❌ 没有获取到复权因子: {ts_code}", pd.DataFrame()
            except Exception as adj_error:
                return False, f"❌ 复权因子获取失败: {ts_code} - {str(adj_error)}", pd.DataFrame()

            # 合并复权因子
            try:
                df = df.merge(adj_factors, on=['trade_date', 'ts_code'], how='left')
                
                # 检查合并后是否有缺失的复权因子
                if df['adj_factor'].isnull().any():
                    missing_dates = df[df['adj_factor'].isnull()]['trade_date'].tolist()
                    print(f"⚠️ 警告：以下日期缺少复权因子：{', '.join(missing_dates)}")
                
                # 更新列选择以包括 adj_factor
                df = df[["trade_date", "ts_code", "open", "high", "low", "close", "vol", "amount", "adj_factor"]]
                df = df.sort_values("trade_date")
            except Exception as merge_error:
                return False, f"❌ 数据合并失败: {ts_code} - {str(merge_error)}", pd.DataFrame()
            
            new_records = []
            
            try:
                with self.db_manager.get_connection() as conn:
                    for _, row in df.iterrows():
                        trade_date = row["trade_date"]
                        
                        # Check for existing record
                        existing = conn.execute("""
                            SELECT trade_date, ts_code, open, high, low, close, vol, amount, adj_factor 
                            FROM daily_data
                            WHERE ts_code = ? AND trade_date = ?
                        """, (ts_code, trade_date)).fetchone()
                        
                        if existing:
                            # 将查询结果转换为字典，使用列名访问
                            existing_dict = dict(zip(['trade_date', 'ts_code', 'open', 'high', 'low', 'close', 'vol', 'amount', 'adj_factor'], existing))
                            
                            # Compare fields
                            db_row = {
                                "open": round(float(existing_dict['open']), 6),
                                "high": round(float(existing_dict['high']), 6),
                                "low": round(float(existing_dict['low']), 6),
                                "close": round(float(existing_dict['close']), 6),
                                "vol": round(float(existing_dict['vol']), 6),
                                "amount": round(float(existing_dict['amount']), 6),
                                "adj_factor": round(float(existing_dict['adj_factor']), 6)
                            }
                            
                            mem_row = {
                                "open": round(float(row["open"]), 6),
                                "high": round(float(row["high"]), 6),
                                "low": round(float(row["low"]), 6),
                                "close": round(float(row["close"]), 6),
                                "vol": round(float(row["vol"]), 6),
                                "amount": round(float(row["amount"]), 6),
                                "adj_factor": round(float(row["adj_factor"]), 6)
                            }
                            
                            if db_row != mem_row:
                                return False, f"⚠️ 数据冲突：{ts_code} {trade_date} 不一致，数据库={db_row}, 下载={mem_row}", pd.DataFrame()
                        else:
                            new_records.append(row)
                    
                    # Insert new records
                    if new_records:
                        insert_df = pd.DataFrame(new_records)
                        insert_df.to_sql("daily_data", conn, if_exists="append", index=False)
                        return True, f"✅ 插入 {len(insert_df)} 条新记录：{ts_code}", insert_df
                    else:
                        return True, f"✅ 无需插入，数据一致：{ts_code}", df
            except Exception as db_error:
                return False, f"❌ 数据库操作失败: {ts_code} - {str(db_error)}", pd.DataFrame()
                
        except Exception as e:
            return False, f"❌ 未知错误：{str(e)}", pd.DataFrame()

    def get_adj_factor(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        获取复权因子
        
        Args:
            ts_code: 股票代码 (e.g., '000001.SZ')
            start_date: 开始日期，格式：YYYYMMDD
            end_date: 结束日期，格式：YYYYMMDD
            
        Returns:
            pd.DataFrame: 包含复权因子的DataFrame
        """
        try:
            # 直接按日期范围获取复权因子，避免逐日调用API
            df = ts.pro_api().adj_factor(ts_code=ts_code, start_date=start_date, end_date=end_date)
            
            if df is None or df.empty:
                # 如果按范围获取失败，尝试不指定日期范围获取全部数据后筛选
                df_all = ts.pro_api().adj_factor(ts_code=ts_code)
                if df_all is not None and not df_all.empty:
                    # 筛选日期范围内的数据
                    df = df_all[
                        (df_all['trade_date'] >= start_date) & 
                        (df_all['trade_date'] <= end_date)
                    ]
            
            if df is None or df.empty:
                raise ValueError(f"无法获取复权因子: {ts_code}")
                
            # 去重并按日期排序
            df = df.drop_duplicates().sort_values('trade_date')
            return df
            
        except Exception as e:
            raise ValueError(f"获取复权因子失败: {str(e)}")

# Example usage
if __name__ == "__main__":
    loader = TushareLoader()
    success, message, data = loader.download_and_store("601318.SH")
    print(message)
