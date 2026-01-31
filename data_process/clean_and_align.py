import pandas as pd
import os
from tqdm import tqdm
import warnings

# 忽略警告
warnings.simplefilter(action='ignore', category=FutureWarning)

# --- 动态获取项目根目录 ---
current_script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_script_dir)

SOURCE_DIR = os.path.join(project_root, "us_stocks_data")
OUTPUT_DIR = os.path.join(project_root, "data_process")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "full_market_data.parquet")

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

def run_data_processing():
    all_files = [f for f in os.listdir(SOURCE_DIR) if f.lower().endswith('.csv')]
    print(f"📂 数据源: {SOURCE_DIR}")
    print(f"🔍 扫描到 {len(all_files)} 个 CSV 文件。")

    if not all_files:
        print("❌ 未找到文件，请检查路径。")
        return

    # 1. 批量读取与清洗
    all_aligned_data = []
    print(f"🚀 开始清洗与读取 (Batch Processing)...")

    for filename in tqdm(all_files):
        try:
            file_path = os.path.join(SOURCE_DIR, filename)
            ticker = filename.replace('_1h.csv', '').replace('_1H.csv', '')
            
            # 读取 CSV
            df = pd.read_csv(file_path)
            if df.empty: continue

            # --- 核心修复逻辑：剔除垃圾行 ---
            # 如果第一行包含 'Ticker'，说明是元数据行，切片删除前2行
            if 'Ticker' in str(df.iloc[0, 0]):
                df = df.iloc[2:].copy()
                # 强制重命名第一列
                df.rename(columns={df.columns[0]: 'Datetime'}, inplace=True)
            
            # 确保 Datetime 列名存在 (防止某些文件第一列不叫 Ticker 也不叫 Price)
            if 'Datetime' not in df.columns and 'Date' in df.columns:
                df.rename(columns={'Date': 'Datetime'}, inplace=True)
            
            # --- 类型强制转换 ---
            # 1. 时间列清洗
            df['Datetime'] = pd.to_datetime(df['Datetime'], utc=True, errors='coerce')
            df = df.dropna(subset=['Datetime']) # 删掉时间解析失败的行

            # 2. 数值列清洗 (转为 float)
            num_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
            for c in num_cols:
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors='coerce')
            
            if df.empty: continue

            # 标记代码
            df['Ticker'] = ticker
            
            # 选取标准列
            target_cols = ['Datetime', 'Ticker', 'Open', 'High', 'Low', 'Close', 'Volume']
            # 补全缺失列
            for c in target_cols:
                if c not in df.columns: df[c] = None
            
            all_aligned_data.append(df[target_cols])

        except Exception as e:
            continue

    if not all_aligned_data:
        print("❌ 错误：有效数据集为空！")
        return

    # 2. 合并大数据
    print(f"\n📦 正在合并 {len(all_aligned_data)} 个有效文件...")
    final_df = pd.concat(all_aligned_data, ignore_index=True)
    
    # 释放内存
    del all_aligned_data

    # 3. 最终清洗 (修复报错的关键步骤)
    print("🧹 正在执行最终排序与填充...")
    final_df = final_df.sort_values(['Ticker', 'Datetime'])
    
    # 价格列：使用 GroupBy + ffill (前向填充，延续上一个价格)
    price_cols = ['Open', 'High', 'Low', 'Close']
    final_df[price_cols] = final_df.groupby('Ticker')[price_cols].ffill()
    
    # 成交量：直接全局填充 0 (不需要 GroupBy，缺失就是没量)
    # 【修复点】：这里去掉了 .groupby('Ticker')，解决了 AttributeError
    final_df['Volume'] = final_df['Volume'].fillna(0)

    # 4. 保存
    print(f"💾 正在保存至: {OUTPUT_FILE}")
    final_df.to_parquet(OUTPUT_FILE, engine='pyarrow', compression='snappy')
    
    print(f"✨ 成功！最终数据集形状: {final_df.shape}")
    print(f"   (包含 {final_df['Ticker'].nunique()} 只股票)")

if __name__ == "__main__":
    run_data_processing()