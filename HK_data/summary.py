import os
import pandas as pd
import glob
from pathlib import Path

# ================= 配置区域 (已更新) =================
# 1. 原始数据路径 (存放 Raw Data CSVs)
HK_RAW_DIR = r"E:\Msc project\STOCK DATA"

# 2. 清洗后的 Parquet 数据路径 (存放 Cleaned Data)
HK_CLEAN_FILE = r"E:\Msc project\Yfinance RPA\HK_data\hk_unified_market.parquet"
# ===================================================

def get_folder_size_mb(folder_path):
    """计算文件夹大小 (MB)"""
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(folder_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            if not os.path.islink(fp):
                total_size += os.path.getsize(fp)
    return total_size / (1024 * 1024)

def get_file_size_mb(file_path):
    """计算单文件大小 (MB)"""
    return os.path.getsize(file_path) / (1024 * 1024)

def scan_hk_assets():
    print("="*60)
    print("🇭🇰 香港市场数据资产审计报告 (HK Data Audit)")
    print("="*60)

    # --- 1. 原始数据统计 (Raw Data) ---
    print(f"\n📂 [Stage 1] 原始数据池: {HK_RAW_DIR}")
    if os.path.exists(HK_RAW_DIR):
        csv_files = glob.glob(os.path.join(HK_RAW_DIR, "*.csv"))
        raw_count = len(csv_files)
        raw_size = get_folder_size_mb(HK_RAW_DIR)
        
        print(f"   - 文件数量: {raw_count} 个 CSV")
        print(f"   - 存储占用: {raw_size:.2f} MB")
        
        if raw_count > 0:
            # 抽样检查第一个文件
            try:
                sample = pd.read_csv(csv_files[0], nrows=5)
                cols = list(sample.columns)
                print(f"   - 数据维度: {cols}")
            except:
                pass
    else:
        print("   ❌ 路径不存在！请检查路径是否正确。")

    # --- 2. 清洗后数据统计 (Cleaned Data) ---
    print(f"\n📦 [Stage 2] 结构化数据集: {HK_CLEAN_FILE}")
    if os.path.exists(HK_CLEAN_FILE):
        try:
            # 只读取必要列以加速统计
            df = pd.read_parquet(HK_CLEAN_FILE, columns=['Ticker', 'Datetime'])
            
            clean_rows = len(df)
            clean_tickers = df['Ticker'].nunique()
            clean_size = get_file_size_mb(HK_CLEAN_FILE)
            min_date = df['Datetime'].min()
            max_date = df['Datetime'].max()
            
            print(f"   - 有效股票数: {clean_tickers} 只 (经过清洗去重)")
            print(f"   - 总数据行数: {clean_rows:,} 行 (OHLCV)")
            print(f"   - 文件大小:   {clean_size:.2f} MB (Parquet 压缩高效存储)")
            print(f"   - 时间跨度:   {min_date} 至 {max_date}")
            
            # 压缩率计算
            if 'raw_size' in locals() and raw_size > 0:
                ratio = (clean_size / raw_size) * 100
                print(f"   - 存储压缩率: 约为原始体积的 {ratio:.1f}% (更高效)")
                
        except Exception as e:
            print(f"   ⚠️ 读取失败: {e}")
    else:
        print("   ❌ Parquet 文件不存在！")

    print("\n" + "="*60)
    print("✅ 统计完成。请将上述数字填入下方的汇报模板中。")

if __name__ == "__main__":
    scan_hk_assets()