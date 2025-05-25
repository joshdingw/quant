import tushare as ts
import pandas as pd
import sqlite3
import yaml
import os

# === Step 1: 加载配置文件 ===
with open('../Config/config.yaml', 'r') as f:
    config = yaml.safe_load(f)

ts.set_token(config['tushare_token'])
pro = ts.pro_api()
start_date = config.get('start_date', '20240101')
end_date = config.get('end_date', '20240501')

# === Step 2: 连接数据库 ===
os.makedirs('../Database', exist_ok=True)
conn = sqlite3.connect('../Database/history.db')

# === Step 3: 创建表 daily_data（仅保存原始行情数据）===
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
conn.execute(create_table_sql)
conn.commit()

# === Step 4: 下载指定股票数据并存入表中 ===
def download_and_store(ts_code):
    df = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
    if df.empty:
        print(f"❌ 没有获取到数据: {ts_code}")
        return

    df = df.sort_values("trade_date")
    df = df[["trade_date", "ts_code", "open", "high", "low", "close", "vol", "amount"]]

    new_records = []  # 收集需要写入的新记录

    for _, row in df.iterrows():
        trade_date = row["trade_date"]

        # 查询是否已有记录
        existing = conn.execute("""
            SELECT * FROM daily_data
            WHERE ts_code = ? AND trade_date = ?
        """, (ts_code, trade_date)).fetchone()

        if existing:
            # 比较字段是否一致
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

            if db_row == mem_row:
                # 完全一致，跳过这条
                continue
            else:
                raise ValueError(
                    f"⚠️ 数据冲突：{ts_code} {trade_date} 不一致，数据库={db_row}, 下载={mem_row}"
                )
        else:
            # 新记录，添加到待插入列表
            new_records.append(row)

    # 插入所有新记录
    if new_records:
        insert_df = pd.DataFrame(new_records)
        insert_df.to_sql("daily_data", conn, if_exists="append", index=False)
        print(f"✅ 插入 {len(insert_df)} 条新记录：{ts_code}")
    else:
        print(f"✅ 无需插入，数据一致：{ts_code}")

# === Step 5: 下载一只股票进行测试 ===
download_and_store("601318.SH")

conn.close()