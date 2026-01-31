import os
import shutil
from datetime import datetime

# 配置
BASE_DIR = "./us_stocks_data"
PROGRESS_FILE = "progress.txt"

def clean_ghost_entries():
    print("=" * 50)
    print("🧹 开始执行进度文件清洗 (Sync Check)")
    print("=" * 50)

    # 1. 获取 progress.txt 中的名单
    if not os.path.exists(PROGRESS_FILE):
        print("❌ 未找到 progress.txt，无法执行清洗。")
        return
    
    with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
        recorded_tickers = set(line.strip() for line in f if line.strip())
    
    print(f"📋 progress.txt 记录数: {len(recorded_tickers)}")

    # 2. 获取 us_stocks_data 文件夹里的实际 CSV 名单
    if not os.path.exists(BASE_DIR):
        print("❌ 数据文件夹不存在。")
        return

    files = os.listdir(BASE_DIR)
    # 提取文件名中的股票代码 (假设文件名格式为 "AAPL_1h.csv")
    actual_files = set(f.split('_1h.csv')[0] for f in files if f.endswith('.csv'))
    
    print(f"📂 实际 CSV 文件数:     {len(actual_files)}")

    # 3. 找出“幽灵”数据 (在 txt 里，但没 CSV 的)
    # 这些可能是退市股，也可能是因为断网漏下的
    ghosts = recorded_tickers - actual_files
    
    print("-" * 50)
    if not ghosts:
        print("✅ 完美同步！没有发现幽灵数据。")
        return

    print(f"👻 发现 {len(ghosts)} 个记录存在但无文件的股票。")
    print(f"   示例: {list(ghosts)[:10]} ...")
    
    # 4. 执行清洗
    user_input = input("\n⚠️ 是否要从 progress.txt 中删除这些记录，以便重新抓取它们？(y/n): ")
    
    if user_input.lower() == 'y':
        # 备份原文件
        backup_name = f"progress_backup_{datetime.now().strftime('%Y%m%d%H%M')}.txt"
        shutil.copy(PROGRESS_FILE, backup_name)
        print(f"📦 已备份原文件为: {backup_name}")

        # 重写 progress.txt，只保留那些真正有 CSV 的代码
        # 注意：这里我们选择“只保留有文件的”，这意味着那 900 多个退市股
        # 在下次运行时会被【重新检测】。这是最安全的做法。
        with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
            for ticker in sorted(list(actual_files)):
                f.write(f"{ticker}\n")
        
        print(f"✅ 清洗完成！progress.txt 现包含 {len(actual_files)} 行。")
        print("🚀 现在，你可以再次运行 test.py，它将重新尝试这 {len(ghosts)} 个股票。")
    
    else:
        print("🚫 操作已取消，文件未修改。")

if __name__ == "__main__":
    clean_ghost_entries()