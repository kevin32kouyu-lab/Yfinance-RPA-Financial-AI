import pandas as pd
import numpy as np
import os

# --- 路径配置 ---
current_script_dir = os.path.dirname(os.path.abspath(__file__))
# 自动定位到 output 文件夹
PROJECT_ROOT = current_script_dir # 假设你在根目录运行，如果不是请调整
if "data_process" in current_script_dir:
    PROJECT_ROOT = os.path.dirname(current_script_dir)

RAW_FILE = os.path.join(PROJECT_ROOT, "data_process", "full_market_data.parquet")
PROCESSED_FILE = os.path.join(PROJECT_ROOT, "output", "engineered_features.parquet")

def run_investigation():
    print("="*50)
    print("🕵️‍♂️ 开始调查数据丢失原因 (Data Investigation)")
    print("="*50)

    # 1. 加载两份名单
    if not os.path.exists(RAW_FILE) or not os.path.exists(PROCESSED_FILE):
        print("❌ 缺少必要文件，无法对比。")
        return

    print("📂 读取原始数据名单 (Full Market Data)...")
    # 只读 Ticker 列以节省内存
    df_raw = pd.read_parquet(RAW_FILE, columns=['Ticker', 'Datetime'])
    raw_tickers = set(df_raw['Ticker'].unique())
    print(f"   - 原始股票数: {len(raw_tickers)}")

    print("📂 读取特征数据名单 (Engineered Features)...")
    df_proc = pd.read_parquet(PROCESSED_FILE, columns=['Ticker'])
    proc_tickers = set(df_proc['Ticker'].unique())
    print(f"   - 幸存股票数: {len(proc_tickers)}")

    # 2. 找出“受害者”
    dropped_tickers = raw_tickers - proc_tickers
    print(f"💀 被删除股票数: {len(dropped_tickers)}")
    
    if len(dropped_tickers) == 0:
        print("✅ 没有股票被删除，无需调查。")
        return

    # 3. 核心取证：统计这些被删股票的原始长度
    print("\n🔍 正在检查被删股票的原始数据长度...")
    
    # 计算原始数据中每只股票的行数
    # value_counts 比 groupby 快很多
    raw_counts = df_raw['Ticker'].value_counts()
    
    # 提取被删股票的长度
    dropped_counts = raw_counts[list(dropped_tickers)]
    
    # 4. 判决时刻
    THRESHOLD = 50
    short_stocks = dropped_counts[dropped_counts < THRESHOLD]
    other_reasons = dropped_counts[dropped_counts >= THRESHOLD]
    
    print("-" * 30)
    print("📊 调查结果报告")
    print("-" * 30)
    print(f"📉 因为长度不足 (<{THRESHOLD}行) 被删的数量: {len(short_stocks)}")
    print(f"❓ 因为其他原因被删的数量: {len(other_reasons)}")
    
    # 5. 展示证据细节
    if len(short_stocks) > 0:
        print(f"\n📝 证据抽样 (长度不足的):")
        print(short_stocks.head(10).to_string())
        print("...")

    if len(other_reasons) > 0:
        print(f"\n⚠️ 警告：发现 {len(other_reasons)} 只股票长度足够却被删了！")
        print("这说明可能存在数据质量问题 (如全是空值、停牌等)。")
        print("详细名单:")
        print(other_reasons.head())
    else:
        print("\n✅ 结论验证：所有被删股票确实都是因为数据历史太短。")

if __name__ == "__main__":
    run_investigation()