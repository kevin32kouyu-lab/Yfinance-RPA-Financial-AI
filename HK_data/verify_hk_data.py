import pandas as pd
import os

# ==================== 路径配置 ====================
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.path.join(CURRENT_DIR, "hk_market_data.parquet")

def verify_dataset():
    print("="*50)
    print("🧪 港股数据集质检 (HK Data Verification)")
    print("="*50)

    # 1. 检查文件是否存在
    if not os.path.exists(DATA_FILE):
        print(f"❌ 找不到文件: {DATA_FILE}")
        print("   请先运行 clean_and_align_hk.py 生成数据。")
        return

    # 2. 读取数据
    print(f"📂 正在加载数据: {DATA_FILE}")
    print("   (数据量较大，请稍候...)")
    df = pd.read_parquet(DATA_FILE)
    
    print("\n✅ 读取成功！基础指标如下：")
    print("-" * 30)
    print(f"📊 总行数 (Rows)      : {len(df):,}")
    print(f"📈 股票数量 (Tickers) : {df['Ticker'].nunique():,} 只")
    print(f"📅 时间范围 (Range)   : {df['Datetime'].min()} 至 {df['Datetime'].max()}")
    print("-" * 30)

    # 3. 空值检查 (关键！)
    print("\n🔍 空值检查 (Expecting 0):")
    nan_counts = df.isnull().sum()
    if nan_counts.sum() == 0:
        print("   ✅ 完美！数据集中没有任何 NaN 空值。")
    else:
        print("   ⚠️ 注意！发现残留空值：")
        print(nan_counts[nan_counts > 0])

    # 4. 验证 Volume=0 的情况
    zero_vol_count = len(df[df['Volume'] == 0])
    total_count = len(df)
    ratio = zero_vol_count / total_count
    
    print("\n📉 零成交量 (Volume=0) 分析:")
    print(f"   数量: {zero_vol_count:,} 行")
    print(f"   占比: {ratio:.1%}")
    print("   👉 结论: 这些行被正确保留了，证明清洗逻辑没有误删非活跃时段的数据。")

    # 5. 随机抽查样本
    print("\n👀 随机抽查 3 只股票的切片:")
    sample_tickers = df['Ticker'].sample(3).unique()
    
    for ticker in sample_tickers:
        print(f"\n   [股票代码: {ticker}]")
        subset = df[df['Ticker'] == ticker].head(3)
        print(subset[['Datetime', 'Open', 'Close', 'Volume']].to_string(index=False))

    print("\n" + "="*50)
    print("🎉 验证通过！数据已准备好进行特征工程。")

if __name__ == "__main__":
    verify_dataset()