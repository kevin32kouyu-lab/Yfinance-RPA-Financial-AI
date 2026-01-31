import os
import pandas as pd
from datetime import datetime

# 配置路径（确保与你的采集脚本一致）
BASE_DIR = "./us_stocks_data"
PROGRESS_FILE = "progress.txt"
REPORT_NAME = "failure_report.csv"

def generate_failure_report():
    print("=" * 50)
    print(f"📊 开始扫描缺失股票清单... ({datetime.now().strftime('%H:%M:%S')})")
    print("=" * 50)

    # 1. 加载 progress.txt 中的名单
    if not os.path.exists(PROGRESS_FILE):
        print(f"❌ 错误：找不到进度文件 {PROGRESS_FILE}")
        return
    
    with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
        # 使用 set 提高查找效率
        recorded_tickers = set(line.strip() for line in f if line.strip())
    
    print(f"✅ 已记录的总尝试数: {len(recorded_tickers)}")

    # 2. 扫描数据文件夹中的实际 CSV 文件
    if not os.path.exists(BASE_DIR):
        print(f"❌ 错误：找不到数据目录 {BASE_DIR}")
        return
    
    # 提取已经存在的股票代码
    files = os.listdir(BASE_DIR)
    success_tickers = set(f.split('_1h.csv')[0] for f in files if f.endswith('.csv'))
    
    print(f"✅ 实际下载成功的数量: {len(success_tickers)}")

    # 3. 找出“失败/无数据”的股票
    # 逻辑：在 progress.txt 中出现过，但在文件夹里没找到 CSV 的
    failed_tickers = sorted(list(recorded_tickers - success_tickers))
    
    print(f"❌ 识别到无数据/失败的股票数: {len(failed_tickers)}")

    # 4. 导出为报告文件
    if failed_tickers:
        df = pd.DataFrame(failed_tickers, columns=['Ticker'])
        df['Status'] = 'No Data / Delisted / Network Error'
        df['Check_Time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        df.to_csv(REPORT_NAME, index=False)
        print("-" * 50)
        print(f"💾 报告已生成: {os.path.abspath(REPORT_NAME)}")
        print(f"💡 你可以直接打开这个 CSV 查看所有未能下载成功的股票。")
        
        # 打印前 10 个作为预览
        print(f"📝 失败示例预览: {failed_tickers[:10]}")
    else:
        print("🎉 恭喜！所有在进度表中的股票都成功生成了 CSV 文件。")

if __name__ == "__main__":
    generate_failure_report()