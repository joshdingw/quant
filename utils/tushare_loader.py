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
    def __init__(self, config_path: str = '../Config/config.yaml'):
        """
        Initialize TushareLoader with configuration
        
        Args:
            config_path: Path to the config file containing Tushare token and other settings
        """
        # Load configuration
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        # Initialize Tushare
        ts.set_token(self.config['tushare_token'])
        self.pro = ts.pro_api()
        
        # Set default date range
        self.start_date = self.config.get('start_date', '20240101')
        self.end_date = self.config.get('end_date', '20240501')
        
        # Initialize database manager
        self.db_manager = DatabaseManager()
    
    def get_trading_calendar(self, start_date: str, end_date: str) -> pd.DataFrame:
        """
        从Tushare获取交易日历
        
        Args:
            start_date: 开始日期，格式：YYYYMMDD
            end_date: 结束日期，格式：YYYYMMDD
            
        Returns:
            pd.DataFrame: 包含交易日历的DataFrame，包含'cal_date'列
        """
        try:
            # 调用Tushare的交易日历接口
            df = self.pro.trade_cal(exchange='SSE', start_date=start_date, end_date=end_date)
            if df.empty:
                raise ValueError(f"无法获取交易日历: {start_date} 到 {end_date}")
            
            # 只保留交易日
            df = df[df['is_open'] == 1]
            
            # 重命名日期列为cal_date
            df = df.rename(columns={'cal_date': 'cal_date'})
            
            return df[['cal_date']]
            
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
            df = self.pro.daily(ts_code=ts_code, start_date=start, end_date=end)
            if df.empty:
                return False, f"❌ 没有获取到数据: {ts_code}", pd.DataFrame()
            
            df = df.sort_values("trade_date")
            df = df[["trade_date", "ts_code", "open", "high", "low", "close", "vol", "amount"]]
            
            new_records = []
            
            with self.db_manager.get_connection() as conn:
                for _, row in df.iterrows():
                    trade_date = row["trade_date"]
                    
                    # Check for existing record
                    existing = conn.execute("""
                        SELECT * FROM daily_data
                        WHERE ts_code = ? AND trade_date = ?
                    """, (ts_code, trade_date)).fetchone()
                    
                    if existing:
                        # Compare fields
                        db_row = {
                            "open": round(float(existing[2]), 6),
                            "high": round(float(existing[3]), 6),
                            "low": round(float(existing[4]), 6),
                            "close": round(float(existing[5]), 6),
                            "vol": round(float(existing[6]), 6),
                            "amount": round(float(existing[7]), 6),
                        }
                        
                        mem_row = {
                            "open": round(float(row["open"]), 6),
                            "high": round(float(row["high"]), 6),
                            "low": round(float(row["low"]), 6),
                            "close": round(float(row["close"]), 6),
                            "vol": round(float(row["vol"]), 6),
                            "amount": round(float(row["amount"]), 6),
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
                
        except Exception as e:
            return False, f"❌ 错误：{str(e)}", pd.DataFrame()

# Example usage
if __name__ == "__main__":
    loader = TushareLoader()
    success, message, data = loader.download_and_store("601318.SH")
    print(message)
