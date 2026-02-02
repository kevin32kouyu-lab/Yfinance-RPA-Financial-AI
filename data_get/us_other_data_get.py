import yfinance as yf
import pandas as pd
import os
import time
import random
import requests
import io
from tqdm import tqdm

# ==================== 1. 配置区域 ====================
PROXY_URL = 'http://127.0.0.1:10808'

# 路径配置
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
US_DATA_ROOT = os.path.join(PROJECT_ROOT, "us_stocks_data") 

# 保存位置
DIRS = {
    "ETF": os.path.join(US_DATA_ROOT, "us_etf_1h"),
    "FUTURE": os.path.join(US_DATA_ROOT, "us_future_1h")
}

for d in DIRS.values():
    os.makedirs(d, exist_ok=True)

# [新增] 失败日志文件
FAILED_LOG_FILE = os.path.join(CURRENT_DIR, "us_mining_failed.csv")

# 进度文件 (用于去重)
PROGRESS_FILE = os.path.join(CURRENT_DIR, "progress.txt")

# ==================== 2. 环境初始化 ====================
os.environ['HTTP_PROXY'] = PROXY_URL
os.environ['HTTPS_PROXY'] = PROXY_URL

# ==================== 3. 获取目标清单 ====================

def get_existing_tickers():
    if not os.path.exists(PROGRESS_FILE):
        return set()
    with open(PROGRESS_FILE, 'r') as f:
        existing = {line.strip() for line in f if line.strip()}
    print(f"📋 现有库存(Progress): {len(existing)} 只")
    return existing

def get_us_etf_list():
    print("\n📡 正在获取美股 ETF 名单...")
    url = "https://raw.githubusercontent.com/rreichel3/US-Stock-Symbols/main/etf/etf_symbols_list.csv"
    try:
        resp = requests.get(url, timeout=30)
        if resp.status_code == 200:
            df = pd.read_csv(io.StringIO(resp.text))
            tickers = df['symbol'].dropna().unique().tolist()
            print(f"✅ 获取成功: {len(tickers)} 只")
            return tickers
    except Exception as e:
        print(f"❌ 获取失败: {e}")
    
    print("⚠️ 使用内置核心 ETF 列表作为备用")
    return ["SPY", "QQQ", "IWM", "GLD", "SLV", "TQQQ", "SQQQ", "ARKK", "XLK", "XLF", "XLE", "VXX"]

def get_futures_list():
    # 核心期货白名单
    return [
        "ES=F", "NQ=F", "YM=F", "RTY=F", "VIX=F", # 指数
        "CL=F", "NG=F", "BZ=F",                   # 能源
        "GC=F", "SI=F", "HG=F",                   # 金属
        "ZN=F", "ZB=F",                           # 利率
        "DX=F", "6E=F", "6J=F",                   # 外汇
        "BTC=F", "ETH=F"                          # 加密
    ]

# ==================== 4. 下载核心逻辑 (带日志) ====================

def download_batch(category_name, tickers, save_dir, existing_set):
    # 过滤逻辑
    targets = []
    for t in tickers:
        if t in existing_set: continue
        # 期货文件名特殊处理 (= -> _)
        fname = t.replace('=', '_') + "_1h.csv"
        if os.path.exists(os.path.join(save_dir, fname)): continue
        targets.append(t)
        
    print(f"\n🚀 [{category_name}] 任务: {len(targets)} / {len(tickers)}")
    
    if not targets:
        return []

    success_count = 0
    failed_list = [] # [新增] 收集失败记录
    
    pbar = tqdm(targets, unit="code")
    
    for ticker in pbar:
        try:
            pbar.set_description(f"⬇️ {category_name}: {ticker}")
            
            # 下载
            df = yf.download(ticker, period="2y", interval="1h", progress=False)
            
            if df.empty:
                # [记录] 空数据
                failed_list.append({"Category": category_name, "Ticker": ticker, "Reason": "Empty Data"})
                continue

            # 处理
            if df.index.tz is None:
                df.index = df.index.tz_localize('UTC')
            df.index = df.index.tz_convert('America/New_York')

            df.reset_index(inplace=True)
            df['Ticker'] = ticker
            
            # 保存文件名处理
            safe_filename = ticker.replace('=', '_') + "_1h.csv"
            file_path = os.path.join(save_dir, safe_filename)
            
            # 列处理
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            
            cols = ['Datetime', 'Ticker', 'Open', 'High', 'Low', 'Close', 'Volume']
            save_df = df[[c for c in cols if c in df.columns]]
            
            if not save_df.empty:
                save_df.to_csv(file_path, index=False)
                success_count += 1
            else:
                failed_list.append({"Category": category_name, "Ticker": ticker, "Reason": "No Columns"})

            time.sleep(random.uniform(0.5, 1.5))

        except Exception as e:
            # [记录] 报错
            err_msg = str(e)[:100]
            failed_list.append({"Category": category_name, "Ticker": ticker, "Reason": err_msg})
            time.sleep(1)

    print(f"🏁 [{category_name}] 完成: ✅成功 {success_count} | ❌失败 {len(failed_list)}")
    return failed_list

# ==================== 5. 主程序 ====================

def main():
    print("="*50)
    print("🇺🇸 美股全市场挖掘 (V2 - With Log)")
    print("="*50)
    
    existing = get_existing_tickers()
    etf_list = get_us_etf_list()
    future_list = get_futures_list()
    
    all_failures = []
    
    # 执行并收集失败记录
    fails_fut = download_batch("Futures", future_list, DIRS["FUTURE"], existing)
    all_failures.extend(fails_fut)
    
    fails_etf = download_batch("ETFs", etf_list, DIRS["ETF"], existing)
    all_failures.extend(fails_etf)
    
    # [新增] 保存失败日志
    if all_failures:
        log_df = pd.DataFrame(all_failures)
        log_df.to_csv(FAILED_LOG_FILE, index=False)
        print(f"\n📝 失败日志已保存至: {FAILED_LOG_FILE}")
        print(f"   (共记录 {len(all_failures)} 条异常)")
    else:
        print("\n✨ 完美运行！无任何失败记录。")

    print("\n👋 任务结束。")

if __name__ == "__main__":
    main()