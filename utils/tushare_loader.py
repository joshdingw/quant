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

    def download_index_data(self, ts_code: str, start_date: Optional[str] = None, 
                           end_date: Optional[str] = None) -> Tuple[bool, str, pd.DataFrame]:
        """
        Download index data for a given ts_code using index_daily interface
        
        Args:
            ts_code: Index code (e.g., '000300.SH')
            start_date: Start date in YYYYMMDD format (optional)
            end_date: End date in YYYYMMDD format (optional)
            
        Returns:
            Tuple[bool, str, pd.DataFrame]: (success status, message, downloaded data)
        """
        try:
            # Use provided dates or defaults
            start = start_date or self.start_date
            end = end_date or self.end_date
            
            # Download index data using index_daily interface
            try:
                df = ts.pro_api().index_daily(ts_code=ts_code, start_date=start, end_date=end)
                if df is None:
                    return False, f"❌ API返回空数据: {ts_code}", pd.DataFrame()
                if df.empty:
                    return False, f"❌ 指定日期范围内没有指数数据: {ts_code} ({start} 到 {end})", pd.DataFrame()
            except Exception as api_error:
                return False, f"❌ 指数API调用失败: {ts_code} - {str(api_error)}", pd.DataFrame()
            
            # 为指数数据添加 adj_factor 列（指数不需要复权，设为1.0）
            df['adj_factor'] = 1.0
            
            # 重新排列列以匹配股票数据格式
            df = df[["trade_date", "ts_code", "open", "high", "low", "close", "vol", "amount", "adj_factor"]]
            df = df.sort_values("trade_date")
            
            # Store in database (same logic as stock data)
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
                                return False, f"⚠️ 指数数据冲突：{ts_code} {trade_date} 不一致，数据库={db_row}, 下载={mem_row}", pd.DataFrame()
                        else:
                            new_records.append(row)
                    
                    # Insert new records
                    if new_records:
                        insert_df = pd.DataFrame(new_records)
                        insert_df.to_sql("daily_data", conn, if_exists="append", index=False)
                        return True, f"✅ 插入 {len(insert_df)} 条新指数记录：{ts_code}", insert_df
                    else:
                        return True, f"✅ 无需插入，指数数据一致：{ts_code}", df
            except Exception as db_error:
                return False, f"❌ 指数数据库操作失败: {ts_code} - {str(db_error)}", pd.DataFrame()
                
        except Exception as e:
            return False, f"❌ 指数数据下载未知错误：{str(e)}", pd.DataFrame()

    def download_moneyflow_data(self, start_date: Optional[str] = None, 
                               end_date: Optional[str] = None) -> Tuple[bool, str, pd.DataFrame]:
        """
        下载并存储资金流数据
        
        Args:
            start_date: 开始日期，格式：YYYYMMDD (optional)
            end_date: 结束日期，格式：YYYYMMDD (optional)
            
        Returns:
            Tuple[bool, str, pd.DataFrame]: (success status, message, downloaded data)
        """
        try:
            # Use provided dates or defaults
            start = start_date or self.start_date
            end = end_date or self.end_date
            
            # 获取交易日历以按日获取数据
            try:
                trading_days = self.get_trading_calendar(start, end)
                if trading_days.empty:
                    return False, f"❌ 无法获取交易日历：{start} 到 {end}", pd.DataFrame()
            except Exception as calendar_error:
                return False, f"❌ 获取交易日历失败：{str(calendar_error)}", pd.DataFrame()
            
            all_moneyflow_data = []
            successful_days = 0
            failed_days = 0
            
            # 按日期逐天获取资金流数据
            for _, day_row in trading_days.iterrows():
                trade_date = day_row['cal_date']
                
                try:
                    # 获取当日资金流数据
                    daily_df = ts.pro_api().moneyflow(
                        trade_date=trade_date,
                        fields="ts_code,trade_date,buy_elg_amount,buy_elg_vol"
                    )
                    
                    if daily_df is not None and not daily_df.empty:
                        all_moneyflow_data.append(daily_df)
                        successful_days += 1
                        print(f"✅ 成功获取 {trade_date} 资金流数据：{len(daily_df)} 条记录")
                    else:
                        failed_days += 1
                        print(f"⚠️ {trade_date} 无资金流数据")
                        
                except Exception as daily_error:
                    failed_days += 1
                    print(f"❌ {trade_date} 资金流数据获取失败：{str(daily_error)}")
                    continue
            
            if not all_moneyflow_data:
                return False, f"❌ 无法获取任何资金流数据：{start} 到 {end}", pd.DataFrame()
            
            # 合并所有数据
            try:
                combined_df = pd.concat(all_moneyflow_data, ignore_index=True)
                combined_df = combined_df.sort_values(['trade_date', 'ts_code'])
                
                # 确保数据列完整
                required_columns = ['ts_code', 'trade_date', 'buy_elg_amount', 'buy_elg_vol']
                for col in required_columns:
                    if col not in combined_df.columns:
                        return False, f"❌ 资金流数据缺少必要列：{col}", pd.DataFrame()
                
                combined_df = combined_df[required_columns]
                
            except Exception as combine_error:
                return False, f"❌ 合并资金流数据失败：{str(combine_error)}", pd.DataFrame()
            
            # 存储到数据库
            new_records = []
            conflict_records = 0
            
            try:
                with self.db_manager.get_connection() as conn:
                    for _, row in combined_df.iterrows():
                        ts_code = row["ts_code"]
                        trade_date = row["trade_date"]
                        
                        # 检查是否已存在记录
                        existing = conn.execute("""
                            SELECT ts_code, trade_date, buy_elg_amount, buy_elg_vol 
                            FROM moneyflow_data
                            WHERE ts_code = ? AND trade_date = ?
                        """, (ts_code, trade_date)).fetchone()
                        
                        if existing:
                            # 将查询结果转换为字典，使用列名访问
                            existing_dict = dict(zip(['ts_code', 'trade_date', 'buy_elg_amount', 'buy_elg_vol'], existing))
                            
                            # 比较字段
                            db_row = {
                                "buy_elg_amount": round(float(existing_dict['buy_elg_amount'] or 0), 6),
                                "buy_elg_vol": round(float(existing_dict['buy_elg_vol'] or 0), 6)
                            }
                            
                            mem_row = {
                                "buy_elg_amount": round(float(row["buy_elg_amount"] or 0), 6),
                                "buy_elg_vol": round(float(row["buy_elg_vol"] or 0), 6)
                            }
                            
                            if db_row != mem_row:
                                conflict_records += 1
                                print(f"⚠️ 资金流数据冲突：{ts_code} {trade_date}")
                        else:
                            new_records.append(row)
                    
                    # 插入新记录
                    if new_records:
                        insert_df = pd.DataFrame(new_records)
                        insert_df.to_sql("moneyflow_data", conn, if_exists="append", index=False)
                        
                        success_msg = f"✅ 资金流数据获取成功：成功{successful_days}天，失败{failed_days}天，插入{len(insert_df)}条新记录"
                        if conflict_records > 0:
                            success_msg += f"，发现{conflict_records}条冲突记录"
                        
                        return True, success_msg, insert_df
                    else:
                        success_msg = f"✅ 资金流数据一致：成功{successful_days}天，失败{failed_days}天，无需插入新记录"
                        if conflict_records > 0:
                            success_msg += f"，发现{conflict_records}条冲突记录"
                        
                        return True, success_msg, combined_df
                        
            except Exception as db_error:
                return False, f"❌ 资金流数据库操作失败：{str(db_error)}", pd.DataFrame()
                
        except Exception as e:
            return False, f"❌ 资金流数据下载未知错误：{str(e)}", pd.DataFrame()

    def get_stock_basic_info(self) -> Tuple[bool, str, pd.DataFrame]:
        """
        获取A股股票基本信息列表（排除ST股票和北交所股票）
        
        Returns:
            Tuple[bool, str, pd.DataFrame]: (success status, message, stock list)
        """
        try:
            # 获取在市股票
            all_stocks_1 = ts.pro_api().stock_basic(
                exchange='', 
                list_status='L', 
                fields='ts_code,symbol,name,area,industry,list_date'
            )
            
            # 获取退市股票  
            all_stocks_2 = ts.pro_api().stock_basic(
                exchange='', 
                list_status='D', 
                fields='ts_code,symbol,name,area,industry,list_date'
            )
            
            # 获取暂停上市股票
            all_stocks_3 = ts.pro_api().stock_basic(
                exchange='', 
                list_status='P', 
                fields='ts_code,symbol,name,area,industry,list_date'
            )
            
            # 检查数据有效性
            dataframes = [all_stocks_1, all_stocks_2, all_stocks_3]
            valid_dataframes = []
            
            for df in dataframes:
                if df is not None and not df.empty:
                    valid_dataframes.append(df)
            
            if not valid_dataframes:
                return False, "❌ 无法获取任何股票基本信息", pd.DataFrame()
            
            # 合并所有股票数据
            combined_stocks = pd.concat(valid_dataframes, ignore_index=True)
            
            # 排除ST股票
            combined_stocks = combined_stocks[~combined_stocks["name"].str.contains("ST", na=False)]
            
            # 排除北交所股票（.BJ结尾）
            combined_stocks = combined_stocks[~combined_stocks["ts_code"].str.contains(".BJ", na=False)]
            
            # 去重并排序
            stock_codes = sorted(set(combined_stocks['ts_code'].values))
            
            # 返回股票代码列表
            result_df = pd.DataFrame({'ts_code': stock_codes})
            
            return True, f"✅ 获取股票基本信息成功", result_df
            
        except Exception as e:
            return False, f"❌ 获取股票基本信息失败：{str(e)}", pd.DataFrame()

    def get_daily_basic_data(self, trade_date: str, ts_code: Optional[str] = None) -> Tuple[bool, str, pd.DataFrame]:
        """
        获取指定交易日的股票基本信息（包含流通市值等）
        
        Args:
            trade_date: 交易日期，格式：YYYYMMDD
            ts_code: 股票代码（可选），如果不指定则获取所有股票
            
        Returns:
            Tuple[bool, str, pd.DataFrame]: (success status, message, daily basic data)
        """
        try:
            # 调用Tushare的每日基本信息接口
            daily_basic = ts.pro_api().daily_basic(
                trade_date=trade_date,
                ts_code=ts_code,
                fields='ts_code,trade_date,close,turnover_rate,volume_ratio,pe,pb,ps,dv_ratio,dv_ttm,total_share,float_share,free_share,total_mv,circ_mv'
            )
            
            if daily_basic is None or daily_basic.empty:
                return False, f"❌ {trade_date} 无每日基本信息数据", pd.DataFrame()
            
            # 检查必要列是否存在
            required_columns = ['ts_code', 'trade_date', 'circ_mv']
            missing_columns = [col for col in required_columns if col not in daily_basic.columns]
            if missing_columns:
                return False, f"❌ 每日基本信息缺少必要列：{', '.join(missing_columns)}", pd.DataFrame()
            
            # 排序
            daily_basic = daily_basic.sort_values(['ts_code'])
            
            return True, f"✅ 获取{trade_date}每日基本信息成功", daily_basic
            
        except Exception as e:
            return False, f"❌ 获取{trade_date}每日基本信息失败：{str(e)}", pd.DataFrame()

# Example usage
if __name__ == "__main__":
    loader = TushareLoader()
    success, message, data = loader.download_and_store("601318.SH")
    print(message)
