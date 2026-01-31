import os
import requests
import urllib3
import ssl

# 强制降低加密套件的安全等级，以提高与代理的兼容性
ssl._create_default_https_context = ssl._create_unverified_context

# 配置（需与你之前的设置一致）
BASE_DIR = "./us_stocks_data"
PROGRESS_FILE = "progress.txt"
PROXY_URL = 'http://127.0.0.1:10808'

# 设置代理以获取 SEC 名单
os.environ['HTTP_PROXY'] = PROXY_URL
os.environ['HTTPS_PROXY'] = PROXY_URL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def get_sec_tickers():
    """获取目标总名单"""
    print("正在拉取 SEC 全量名单进行比对...")
    session = requests.Session()
    session.proxies = {'http': PROXY_URL, 'https': PROXY_URL}
    session.verify = False
    headers = {'User-Agent': 'MscProject Research (kevin_kou_student@example.com)', 'Host': 'www.sec.gov'}
    
    try:
        resp = session.get("https://www.sec.gov/files/company_tickers.json", headers=headers, timeout=30)
        data = resp.json()
        return set(item['ticker'] for item in data.values())
    except Exception as e:
        print(f"名单获取失败: {e}")
        return set()

def audit_data():
    # 1. 读取各方数据
    sec_tickers = get_sec_tickers()
    
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
            progress_tickers = set(line.strip() for line in f if line.strip())
    else:
        progress_tickers = set()

    # 读取实际下载了 CSV 的文件
    if os.path.exists(BASE_DIR):
        files = os.listdir(BASE_DIR)
        csv_tickers = set(f.split('_1h.csv')[0] for f in files if f.endswith('.csv'))
    else:
        csv_tickers = set()

    # 2. 计算差异
    # 真正漏掉的（既没在 progress.txt 也没下载下来的）
    missing_tickers = sec_tickers - progress_tickers
    
    # 无数据的（在 progress.txt 里，但文件夹里没 CSV，说明是退市或空数据）
    empty_data_tickers = progress_tickers - csv_tickers

    # 3. 输出报告
    print("\n" + "="*40)
    print(f"📊 数据采集审计报告")
    print("="*40)
    print(f"1. SEC 目标总数:  {len(sec_tickers)}")
    print(f"2. 已处理总数:    {len(progress_tickers)} (包含有数据和无数据的)")
    print(f"3. 有效 CSV 文件: {len(csv_tickers)} (实际入库量)")
    print(f"4. 无数据/退市:   {len(empty_data_tickers)} (Yahoo返回空)")
    print("-" * 40)
    print(f"❌ 需重试 (漏网之鱼): {len(missing_tickers)}")
    print("="*40)

    if missing_tickers:
        print("建议：请直接重新运行采集脚本，它会自动处理这", len(missing_tickers), "只股票。")
        # 也可以把漏掉的打印出来看看
        # print(list(missing_tickers)[:10]) 
    else:
        print("✅ 完美！所有股票均已尝试过。")

if __name__ == "__main__":
    audit_data()