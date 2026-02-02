import pandas as pd
import os
from tqdm import tqdm
import warnings

# 忽略警告
warnings.simplefilter(action='ignore', category=FutureWarning)

# ==================== 1. 路径配置 (关键修正) ====================
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)       # Yfinance RPA
MSC_PROJECT_ROOT = os.path.dirname(PROJECT_ROOT)  # Msc project

# [修正] 指向那个有 2730 个文件的正确路径
# 之前是指向: STOCK DATA/us_stocks_data/hk_1h (❌ 只有336个)
# 现在改为:   STOCK DATA/hk_1h             (✅ 有2730个)
SOURCE_DIR = os.path.join(MSC_PROJECT_ROOT, "STOCK DATA", "hk_1h")

# 输出文件
OUTPUT_FILE = os.path.join(CURRENT_DIR, "hk_market_data.parquet")

def run_hk_cleaning_final():
    print("="*50)
    print("🇭🇰 港股数据清洗 (Final Path Fix)")
    print("="*50)
    
    if not os.path.exists(SOURCE_DIR):
        print(f"❌ 找不到数据源: {SOURCE_DIR}")
        return

    csv_files = [f for f in os.listdir(SOURCE_DIR) if f.lower().endswith('.csv')]
    print(f"📂 正确数据源: {SOURCE_DIR}")
    print(f"📝 扫描到文件: {len(csv_files)} 个 (这才是对的！)")
    
    all_data = []
    print(f"🚀 开始读取 (Standard Mode)...")
    
    for filename in tqdm(csv_files):
        try:
            file_path = os.path.join(SOURCE_DIR, filename)
            
            # === 标准读取 ===
            df = pd.read_csv(file_path)
            
            if df.empty: continue
            
            # 1. 检查必要列
            if 'Datetime' not in df.columns:
                continue
                
            # 2. 解析时间 (兼容 UTC 字符串)
            df['Datetime'] = pd.to_datetime(df['Datetime'], utc=True)
            
            # 3. 统一转为香港时间
            df['Datetime'] = df['Datetime'].dt.tz_convert('Asia/Hong_Kong')

            # 4. 提取 Ticker
            ticker = filename.replace('_1h.csv', '').replace('.csv', '') + ".HK"
            df['Ticker'] = ticker
            
            # 5. 统一列顺序并清洗
            cols = ['Datetime', 'Ticker', 'Open', 'High', 'Low', 'Close', 'Volume']
            for c in cols:
                if c not in df.columns: df[c] = None
            
            all_aligned = df[cols].copy()
            # 简单去重，防止有重复行
            all_aligned.drop_duplicates(subset=['Datetime'], inplace=True)
            
            all_data.append(all_aligned)
            
        except Exception:
            continue

    if not all_data:
        print("❌ 没有读取到任何有效数据。")
        return

    # 合并
    print(f"\n📦 正在合并 {len(all_data)} 只股票...")
    final_df = pd.concat(all_data, ignore_index=True)
    
    # 填充逻辑
    print("🧹 执行 Resampling & Filling (修复数据间隙)...")
    final_df = final_df.sort_values(['Ticker', 'Datetime'])
    
    price_cols = ['Open', 'High', 'Low', 'Close']
    final_df[price_cols] = final_df.groupby('Ticker')[price_cols].ffill()
    final_df['Volume'] = final_df['Volume'].fillna(0)
    final_df.dropna(inplace=True)

    # 保存
    print(f"💾 正在保存至: {OUTPUT_FILE}")
    final_df.to_parquet(OUTPUT_FILE, engine='pyarrow', compression='snappy')
    
    print("="*50)
    print("✅ 完美完成！")
    print(f"📊 最终数据形状: {final_df.shape}")
    print(f"📂 文件位置: {OUTPUT_FILE}")

if __name__ == "__main__":
    run_hk_cleaning_final()