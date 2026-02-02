import os

# ==================== 路径配置 ====================
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
MSC_PROJECT_ROOT = os.path.dirname(PROJECT_ROOT)

# 你的数据源路径
SOURCE_DIR = os.path.join(MSC_PROJECT_ROOT, "STOCK DATA", "us_stocks_data", "hk_1h")

def inspect_files():
    print("="*50)
    print("🩺 硬盘文件真实面目检查 (File Inspector)")
    print(f"📂 目标文件夹: {SOURCE_DIR}")
    print("="*50)

    if not os.path.exists(SOURCE_DIR):
        print("❌ 文件夹不存在！")
        return

    files = [f for f in os.listdir(SOURCE_DIR) if f.endswith('.csv')]
    print(f"📝 发现 {len(files)} 个 CSV 文件")

    if not files:
        print("❌ 文件夹是空的。")
        return

    # 抽取前 5 个文件看看它们到底长什么样
    sample_files = files[:5]

    for i, filename in enumerate(sample_files):
        file_path = os.path.join(SOURCE_DIR, filename)
        print(f"\n[{i+1}/5] 正在检查文件: {filename}")
        print("-" * 30)
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                # 读取前 4 行
                lines = [f.readline().strip() for _ in range(4)]
                
            for idx, line in enumerate(lines):
                print(f"   第 {idx+1} 行: {line}")
                
            # 自动判断
            if line[0].startswith("Price"):
                print("   👉 结论: 这是【三层表头】(旧版格式)")
            elif line[0].startswith("Datetime"):
                print("   👉 结论: 这是【标准格式】")
            else:
                print("   👉 结论: 未知/混合格式")
                
        except Exception as e:
            print(f"   ❌ 读取出错: {e}")

if __name__ == "__main__":
    inspect_files()