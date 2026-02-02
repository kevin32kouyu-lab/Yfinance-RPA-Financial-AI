import pandas as pd
import os
from tqdm import tqdm
import warnings

# 忽略警告
warnings.simplefilter(action='ignore', category=FutureWarning)

# ==================== 1. 路径配置 ====================
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
MSC_PROJECT_ROOT = os.path.dirname(PROJECT_ROOT)
STOCK_DATA_ROOT = os.path.join(MSC_PROJECT_ROOT, "STOCK DATA")

# 定义数据源及其对应的资产类型标签
SOURCE_CONFIG = {
    "Stock": os.path.join(STOCK_DATA_ROOT, "hk_1h"),
    "ETF":   os.path.join(STOCK_DATA_ROOT, "hk_etf_1h"),
    "REIT":  os.path.join(STOCK_DATA_ROOT, "hk_reit_1h")
}

# 输出文件
OUTPUT_FILE = os.path.join(CURRENT_DIR, "hk_unified_market.parquet")

def process_folder(asset_type, folder_path):
    """读取指定文件夹下的所有CSV，并打上标签"""
    data_list = []
    
    if not os.path.exists(folder_path):
        print(f"⚠️ 警告: 文件夹不存在 {folder_path}，跳过。")
        return []

    files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
    print(f"📂 正在处理 [{asset_type}]: 找到 {len(files)} 个文件")
    
    for filename in tqdm(files, desc=f"Reading {asset_type}"):
        try:
            file_path = os.path.join(folder_path, filename)
            df = pd.read_csv(file_path)
            
            if df.empty: continue
            
            # 1. 提取 Ticker
            ticker = filename.replace('_1h.csv', '').replace('.csv', '')
            if not ticker.endswith('.HK'):
                ticker += '.HK'
            
            # 2. 确保 Datetime 格式统一
            if 'Datetime' not in df.columns: continue
            
            # [一致性] 这里的逻辑和你原来的脚本完全一样
            df['Datetime'] = pd.to_datetime(df['Datetime'], utc=True)
            df['Datetime'] = df['Datetime'].dt.tz_convert('Asia/Hong_Kong')
            
            # [一致性] 加上去重，防止CSV内部有重复行
            df.drop_duplicates(subset=['Datetime'], inplace=True)

            # 3. 标准化列
            df['Ticker'] = ticker
            df['Asset_Type'] = asset_type  # <--- 唯一的区别：新增身份标签
            
            cols = ['Datetime', 'Ticker', 'Asset_Type', 'Open', 'High', 'Low', 'Close', 'Volume']
            # 补齐缺失列
            for c in cols:
                if c not in df.columns: df[c] = None
                
            data_list.append(df[cols])
            
        except Exception:
            continue
            
    return data_list

def run_unified_merge():
    print("="*50)
    print("🇭🇰 全港股市场统一数据库构建 (Final Compatible Ver)")
    print("="*50)
    
    all_market_data = []
    
    # 1. 依次处理三种资产
    for asset_type, path in SOURCE_CONFIG.items():
        chunk = process_folder(asset_type, path)
        all_market_data.extend(chunk)
        
    if not all_market_data:
        print("❌ 错误：没有读取到任何数据！")
        return

    # 2. 合并
    print(f"\n📦 正在合并 {len(all_market_data)} 个文件片段...")
    final_df = pd.concat(all_market_data, ignore_index=True)
    
    # 3. 清洗 (和你原来的逻辑一致)
    print("🧹 执行最终清洗 (Sorting & Filling)...")
    final_df.sort_values(['Ticker', 'Datetime'], inplace=True)
    
    # 前向填充价格
    price_cols = ['Open', 'High', 'Low', 'Close']
    final_df[price_cols] = final_df.groupby('Ticker')[price_cols].ffill()
    
    # Volume 填 0
    final_df['Volume'] = final_df['Volume'].fillna(0)
    
    # 删除依然为空的行
    final_df.dropna(subset=['Close'], inplace=True)
    
    # 4. 优化内存
    final_df['Asset_Type'] = final_df['Asset_Type'].astype('category')
    final_df['Ticker'] = final_df['Ticker'].astype('category')

    # 5. 保存
    print(f"💾 正在保存至: {OUTPUT_FILE}")
    final_df.to_parquet(OUTPUT_FILE, engine='pyarrow', compression='snappy')
    
    print("="*50)
    print("✅ 统一数据库构建完成！")
    print("-" * 30)
    print(f"📊 总行数: {len(final_df):,}")
    print(f"📈 包含资产数: {final_df['Ticker'].nunique():,}")
    print("   具体分布:")
    print(final_df.groupby('Asset_Type')['Ticker'].nunique().to_string())
    print("-" * 30)

if __name__ == "__main__":
    run_unified_merge()