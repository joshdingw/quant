import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from utils.data_fetcher import DataFetcher
from utils.tushare_loader import TushareLoader

def compare_data():
    # 初始化数据获取器
    fetcher = DataFetcher()
    tushare_loader = TushareLoader()
    
    # 设置参数
    ts_code = "601318.SH"
    start_date = "20230101"
    end_date = "20240301"
    
    # 从数据库获取数据
    db_df, db_message = fetcher.get_stock_data(ts_code, start_date, end_date)
    print("\n数据库数据获取状态:", db_message)
    
    # 直接从Tushare获取数据
    success, message, tushare_df = tushare_loader.download_and_store(ts_code, start_date, end_date)
    print("\nTushare数据获取状态:", message)
    
    if not success or tushare_df.empty:
        print("无法从Tushare获取数据，无法进行比较")
        return
    
    # 确保日期格式一致
    db_df['trade_date'] = pd.to_datetime(db_df['trade_date'])
    tushare_df['trade_date'] = pd.to_datetime(tushare_df['trade_date'])
    
    # 设置日期为索引以便比较
    db_df.set_index('trade_date', inplace=True)
    tushare_df.set_index('trade_date', inplace=True)
    
    # 比较数据
    print("\n数据对比结果:")
    print(f"数据库数据条数: {len(db_df)}")
    print(f"Tushare数据条数: {len(tushare_df)}")
    
    # 检查日期范围
    print(f"\n数据库日期范围: {db_df.index.min()} 到 {db_df.index.max()}")
    print(f"Tushare日期范围: {tushare_df.index.min()} 到 {tushare_df.index.max()}")
    
    # 检查数据是否完全一致
    is_identical = db_df.equals(tushare_df)
    print(f"\n数据是否完全一致: {'是' if is_identical else '否'}")

if __name__ == "__main__":
    compare_data() 