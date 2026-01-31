import yfinance as yf
import pandas as pd
import time
import os
import requests
import random
import urllib3
from datetime import datetime

# ==================== 1. 基础配置 ====================
BASE_DIR = "./us_stocks_data"
PROGRESS_FILE = "progress.txt"
LOG_FILE = "scrape_log.txt"
REPORT_INTERVAL = 50 

# [配置] 代理地址 (保持你的 10808 端口)
PROXY_URL = 'http://127.0.0.1:10808'

# ==================== 2. 网络环境初始化 ====================
# 忽略 SSL 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 【关键】设置系统级代理环境变量
# yfinance 和 requests 都会自动读取这两个变量，无需在函数里传参
os.environ['HTTP_PROXY'] = PROXY_URL
os.environ['HTTPS_PROXY'] = PROXY_URL

# --- SEC 专用 Session (仅用于获取股票名单) ---
sec_session = requests.Session()
# 显式给 SEC Session 设置代理，确保万无一失
sec_session.proxies = {'http': PROXY_URL, 'https': PROXY_URL}
sec_session.verify = False 
SEC_HEADERS = {
    'User-Agent': 'MscProject Research (kevin_kou_student@example.com)',
    'Accept-Encoding': 'gzip, deflate',
    'Host': 'www.sec.gov'
}

# ==================== 3. 核心功能函数 ====================

def init_workspace():
    if not os.path.exists(BASE_DIR):
        os.makedirs(BASE_DIR)
    print(f"工作目录已就绪: {os.path.abspath(BASE_DIR)}")

def get_sec_tickers():
    """从 SEC 获取全量美股代码"""
    url = "https://www.sec.gov/files/company_tickers.json"
    print(f"正在连接 SEC 官网获取股票名单 (代理: {PROXY_URL})...")
    
    try:
        response = sec_session.get(url, headers=SEC_HEADERS, timeout=30)
        
        if response.status_code == 403:
            print("警告: SEC 返回 403，尝试切换 SSL 验证模式...")
            response = sec_session.get(url, headers=SEC_HEADERS, timeout=30, verify=True)

        response.raise_for_status()
        
        data = response.json()
        tickers = sorted(list(set([item['ticker'] for item in data.values()])))
        print(f"✅ 成功获取 {len(tickers)} 只美股代码！")
        return tickers

    except Exception as e:
        print(f"❌ SEC 名单获取失败: {e}")
        print("启动备用方案：仅测试核心科技股...")
        return ["AAPL", "NVDA", "TSLA", "MSFT", "AMZN", "GOOGL", "META"]

def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
            return set(line.strip() for line in f)
    return set()

def save_progress(ticker):
    with open(PROGRESS_FILE, 'a', encoding='utf-8') as f:
        f.write(f"{ticker}\n")

def run_scraper():
    init_workspace()
    
    # 1. 获取名单
    all_tickers = get_sec_tickers()

    # 2. 计算剩余任务
    finished_tickers = load_progress()
    remaining_tickers = [t for t in all_tickers if t not in finished_tickers]
    
    total = len(all_tickers)
    done_count = len(finished_tickers)
    
    print("=" * 60)
    print(f"🚀 任务启动 | 总数: {total} | 待处理: {len(remaining_tickers)}")
    print("=" * 60)

    start_time = time.time()
    session_done = 0

    # 3. 开始循环抓取
    for ticker in remaining_tickers:
        try:
            # =================================================================
            # 最终修复：
            # 1. 移除 proxy 参数 (解决 unexpected keyword argument 'proxy' 报错)
            # 2. 移除 session 参数 (解决 curl_cffi 报错)
            # 3. 依赖 os.environ 全局代理设置
            # =================================================================
            data = yf.download(
                ticker, 
                interval="1h", 
                period="730d", 
                auto_adjust=True, 
                progress=False
            )
            
            if not data.empty:
                # 统一转为纽约时间
                data.index = data.index.tz_convert('America/New_York')
                file_path = os.path.join(BASE_DIR, f"{ticker}_1h.csv")
                data.to_csv(file_path)
            
            # 标记为完成
            save_progress(ticker)
            session_done += 1

            # --- 定时报告 ---
            if session_done % REPORT_INTERVAL == 0:
                cur_total_done = done_count + session_done
                percent = (cur_total_done / total) * 100
                elapsed_min = (time.time() - start_time) / 60
                print(f"📊 [报告] 进度: {percent:.2f}% | 本次耗时: {elapsed_min:.1f}分")

            # 随机休眠
            time.sleep(random.uniform(1.0, 2.0))

        except Exception as e:
            err_msg = str(e)
            print(f"⚠️ {ticker} 失败: {err_msg}")
            
            if "429" in err_msg:
                print("🛑 触发频率限制，强制休息 10 分钟...")
                time.sleep(600)
            continue

    print("\n🎉 所有任务执行完毕！")

if __name__ == "__main__":
    run_scraper()