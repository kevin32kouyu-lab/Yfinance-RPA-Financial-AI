import yfinance as yf
import pandas as pd
import os
import time
import random
import urllib3
import requests
from tqdm import tqdm

# ==================== 1. 配置区域 ====================
PROXY_URL = 'http://127.0.0.1:10808'

# 路径配置
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
MSC_PROJECT_ROOT = os.path.dirname(PROJECT_ROOT)
STOCK_DATA_ROOT = os.path.join(MSC_PROJECT_ROOT, "STOCK DATA")

# 定义三个目标子文件夹
DIRS = {
    "ETF": os.path.join(STOCK_DATA_ROOT, "hk_etf_1h"),
    "REIT": os.path.join(STOCK_DATA_ROOT, "hk_reit_1h"),
    "BOND": os.path.join(STOCK_DATA_ROOT, "hk_bond_1h")
}

for d in DIRS.values():
    os.makedirs(d, exist_ok=True)

FAILED_LOG_FILE = os.path.join(CURRENT_DIR, "hk_funds_bonds_failed.csv")
HKEX_URL = "https://www.hkex.com.hk/eng/services/trading/securities/securitieslists/ListOfSecurities.xlsx"

# ==================== 2. 环境初始化 ====================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
os.environ['HTTP_PROXY'] = PROXY_URL
os.environ['HTTPS_PROXY'] = PROXY_URL

# ==================== 3. 核心功能函数 ====================

def get_categorized_tickers():
    print("📋 正在获取 HKEX 官方证券名单并分类...")
    try:
        resp = requests.get(HKEX_URL, verify=False, timeout=30)
        temp_path = os.path.join(CURRENT_DIR, "hk_list_temp_fb.xlsx")
        with open(temp_path, 'wb') as f:
            f.write(resp.content)
            
        # 读取 CSV (你的文件其实是 CSV 格式)
        try:
            # 尝试直接读取 Excel
            df = pd.read_excel(temp_path, header=2, engine='openpyxl')
        except:
            # 如果失败，尝试作为 CSV 读取 (兼容你上传的那个格式)
            df = pd.read_csv(temp_path, header=2)

        try: os.remove(temp_path)
        except: pass

        if 'Stock Code' not in df.columns or 'Category' not in df.columns:
            print("❌ 官方名单格式有变，无法解析！")
            print(f"   列名: {df.columns.tolist()}")
            return None

        # 清洗 Stock Code
        df['Ticker'] = df['Stock Code'].astype(str).str.extract(r'(\d+)')[0]
        df['Ticker'] = df['Ticker'].str.zfill(4) + ".HK"
        
        # === [修正] 精确分类逻辑 ===
        # 1. ETFs: 对应 'Exchange Traded Products'
        etf_mask = df['Category'] == 'Exchange Traded Products'
        etfs = df[etf_mask]['Ticker'].unique().tolist()
        
        # 2. REITs: 对应 'Real Estate Investment Trusts'
        reit_mask = df['Category'] == 'Real Estate Investment Trusts'
        reits = df[reit_mask]['Ticker'].unique().tolist()
        
        # 3. Bonds: 对应 'Debt Securities'
        bond_mask = df['Category'] == 'Debt Securities'
        bonds = df[bond_mask]['Ticker'].unique().tolist()

        print(f"✅ 分类解析完成 (总计目标: {len(etfs)+len(reits)+len(bonds)}):")
        print(f"   📊 ETFs  : {len(etfs)} 只 (如 2800.HK)")
        print(f"   🏢 REITs : {len(reits)} 只 (如 0823.HK)")
        print(f"   📜 Bonds : {len(bonds)} 只 (注意：债券可能很多为空)")
        
        return {"ETF": etfs, "REIT": reits, "BOND": bonds}
            
    except Exception as e:
        print(f"❌ 获取名单失败: {e}")
        return None

def download_batch(category_name, tickers, save_dir):
    print(f"\n🚀 开始抓取 [{category_name}] (目标: {len(tickers)} 只)")
    
    stats = {"Success": 0, "Skip": 0, "Empty": 0, "Error": 0}
    failed_records = []
    
    pbar = tqdm(tickers, unit="stock")
    
    for ticker in pbar:
        file_name = f"{ticker.replace('.HK', '')}_1h.csv"
        file_path = os.path.join(save_dir, file_name)
        
        if os.path.exists(file_path):
            stats["Skip"] += 1
            continue
            
        try:
            pbar.set_description(f"⬇️ {category_name}: {ticker}")
            
            stock = yf.Ticker(ticker)
            df = stock.history(period="2y", interval="1h")
            
            if df.empty:
                stats["Empty"] += 1
                if category_name != "BOND": # 债券空值太正常了，不记入错误日志以免刷屏
                    failed_records.append({"Category": category_name, "Ticker": ticker, "Reason": "Empty Data"})
                continue
                
            if df.index.tz is None:
                df.index = df.index.tz_localize('UTC')
            df.index = df.index.tz_convert('Asia/Hong_Kong')
            
            df.reset_index(inplace=True)
            df['Ticker'] = ticker
            cols = ['Datetime', 'Ticker', 'Open', 'High', 'Low', 'Close', 'Volume']
            save_df = df[[c for c in cols if c in df.columns]]
            
            if not save_df.empty:
                save_df.to_csv(file_path, index=False)
                stats["Success"] += 1
            else:
                stats["Empty"] += 1

            time.sleep(random.uniform(0.5, 1.5))

        except Exception as e:
            stats["Error"] += 1
            failed_records.append({"Category": category_name, "Ticker": ticker, "Reason": str(e)[:50]})
            time.sleep(1)

    print(f"🏁 [{category_name}] 结束: ✅{stats['Success']} | 📭{stats['Empty']} | ❌{stats['Error']}")
    return failed_records

def main():
    targets = get_categorized_tickers()
    if not targets: return

    all_failed = []
    # 依次执行
    all_failed.extend(download_batch("ETF", targets["ETF"], DIRS["ETF"]))
    all_failed.extend(download_batch("REIT", targets["REIT"], DIRS["REIT"]))
    all_failed.extend(download_batch("BOND", targets["BOND"], DIRS["BOND"]))
    
    if all_failed:
        df_log = pd.DataFrame(all_failed)
        df_log.to_csv(FAILED_LOG_FILE, index=False, encoding='utf-8-sig')
        print(f"\n📝 失败日志: {FAILED_LOG_FILE}")
    
    print("\n✨ 全部完成！")

if __name__ == "__main__":
    main()