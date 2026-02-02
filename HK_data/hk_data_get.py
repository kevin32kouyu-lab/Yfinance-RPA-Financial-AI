import yfinance as yf
import pandas as pd
import os
import time
import random
import urllib3
import requests
from tqdm import tqdm

# ==================== 1. 配置区域 ====================
# [配置] 代理地址
PROXY_URL = 'http://127.0.0.1:10808'

# 路径配置
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR) 

# --- A. 数据文件保存位置 (外层 STOCK DATA) ---
# 自动定位到项目同级的 STOCK DATA 文件夹
STOCK_DATA_ROOT = os.path.join(os.path.dirname(PROJECT_ROOT), "STOCK DATA")
OUTPUT_SUBDIR = os.path.join(STOCK_DATA_ROOT, "hk_1h")
os.makedirs(OUTPUT_SUBDIR, exist_ok=True)

# --- B. 失败日志保存位置 (修改为项目内的 output) ---
INTERNAL_OUTPUT_DIR = os.path.join(PROJECT_ROOT, "output")
os.makedirs(INTERNAL_OUTPUT_DIR, exist_ok=True)

FAILED_LOG_FILE = os.path.join(INTERNAL_OUTPUT_DIR, "hk_failed_log.csv")

HKEX_OFFICIAL_LIST_URL = "https://www.hkex.com.hk/eng/services/trading/securities/securitieslists/ListOfSecurities.xlsx"

# ==================== 2. 环境初始化 ====================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
os.environ['HTTP_PROXY'] = PROXY_URL
os.environ['HTTPS_PROXY'] = PROXY_URL
print(f"🌍 已配置代理: {PROXY_URL}")

# ==================== 3. 核心功能函数 ====================

def get_precise_hk_tickers():
    """获取精准名单（已过滤只保留 Equity）"""
    print("📋 正在获取 HKEX 官方证券名单...")
    try:
        resp = requests.get(HKEX_OFFICIAL_LIST_URL, verify=False, timeout=30)
        temp_path = os.path.join(CURRENT_DIR, "hk_list_temp.xlsx")
        with open(temp_path, 'wb') as f:
            f.write(resp.content)
            
        df = pd.read_excel(temp_path, header=2, engine='openpyxl')
        try: os.remove(temp_path)
        except: pass

        if 'Stock Code' in df.columns and 'Category' in df.columns:
            # 核心过滤：只保留正股 (Equity)
            df_equity = df[df['Category'] == 'Equity'].copy()
            codes = df_equity['Stock Code'].astype(str).str.replace(r'\D', '', regex=True)
            codes = codes[codes.str.len() > 0]
            codes = codes.apply(lambda x: x.zfill(4) + ".HK").unique().tolist()
            print(f"✅ 获取成功！已过滤衍生品，剩余 {len(codes)} 只正股。")
            return codes
            
    except Exception as e:
        print(f"⚠️ 无法获取官方名单 ({e})，切换至回退模式 (0001-0999)...")
    
    return [f"{str(i).zfill(4)}.HK" for i in range(1, 9999)]

def run_safe_download():
    tickers = get_precise_hk_tickers()
    total = len(tickers)
    
    print(f"🚀 开始抓取 (目标: {total} 只 | 带失败统计)")
    print(f"📂 数据存放: {OUTPUT_SUBDIR}")
    print(f"📝 失败日志: {FAILED_LOG_FILE}")
    
    # 统计计数器
    stats = {"Success": 0, "Skip": 0, "Empty": 0, "Error": 0}
    # 失败记录列表
    failed_records = []

    pbar = tqdm(tickers, total=total, unit="stock")
    
    for ticker in pbar:
        file_path = os.path.join(OUTPUT_SUBDIR, f"{ticker.replace('.HK', '')}_1h.csv")
        
        # 1. 跳过已存在
        if os.path.exists(file_path):
            stats["Skip"] += 1
            pbar.set_description(f"⏩ 跳过 {ticker}")
            continue
            
        try:
            pbar.set_description(f"⬇️ 下载 {ticker}")
            
            # 2. 下载数据
            stock = yf.Ticker(ticker)
            df = stock.history(period="2y", interval="1h")
            
            # --- 情况 A: 无数据 ---
            if df.empty:
                stats["Empty"] += 1
                failed_records.append({"Ticker": ticker, "Reason": "Empty Data (No history found)"})
                continue
                
            # 3. 时区转换
            if df.index.tz is None:
                df.index = df.index.tz_localize('UTC')
            df.index = df.index.tz_convert('Asia/Hong_Kong')
            
            # 4. 保存
            df.reset_index(inplace=True)
            df['Ticker'] = ticker
            cols = ['Datetime', 'Ticker', 'Open', 'High', 'Low', 'Close', 'Volume']
            save_df = df[[c for c in cols if c in df.columns]]
            save_df.to_csv(file_path, index=False)
            
            stats["Success"] += 1
            
            # 随机休眠
            time.sleep(random.uniform(1.0, 2.0))

        except Exception as e:
            # --- 情况 B: 报错 ---
            stats["Error"] += 1
            error_msg = str(e).replace('\n', ' ')[:100]
            failed_records.append({"Ticker": ticker, "Reason": f"Error: {error_msg}"})
            time.sleep(1)

    print("\n" + "="*40)
    print(f"📊 任务完成")
    print(f"✅ 成功下载: {stats['Success']}")
    print(f"⏩ 跳过已存: {stats['Skip']}")
    print(f"📭 无数据  : {stats['Empty']}")
    print(f"❌ 发生错误: {stats['Error']}")
    
    # === 保存失败日志 ===
    if failed_records:
        df_failed = pd.DataFrame(failed_records)
        df_failed.to_csv(FAILED_LOG_FILE, index=False, encoding='utf-8-sig')
        print(f"📝 失败详情已保存至: {FAILED_LOG_FILE}")
    else:
        print("🎉 完美！没有失败记录。")
    print("="*40)

if __name__ == "__main__":
    run_safe_download()