import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

# --- 1. 自动寻找数据文件 (复用之前的逻辑) ---
current_dir = os.path.dirname(os.path.abspath(__file__))

# 修正：以项目根为基准查找 output / data_process 下的 parquet 文件
PROJECT_ROOT = os.path.abspath(os.path.join(current_dir, ".."))
possible_paths = [
    os.path.join(PROJECT_ROOT, "output", "engineered_features.parquet"),
    os.path.join(PROJECT_ROOT, "data_process", "full_market_data.parquet"),
    os.path.join(PROJECT_ROOT, "output", "full_market_data.parquet"),
]

DATA_PATH = None
for path in possible_paths:
    if os.path.exists(path):
        DATA_PATH = path
        break

if DATA_PATH is None:
    print("当前未找到 .parquet 数据文件。已尝试路径：")
    for p in possible_paths:
        print("  -", p)

OUTPUT_DIR = os.path.join(current_dir, "output", "report_images")
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

# 设置绘图风格
plt.style.use('ggplot')
sns.set_context("talk")
plt.rcParams['font.sans-serif'] = ['Arial']

def analyze_survivorship():
    print("="*50)
    print("🕵️‍♂️ 股票生存周期分析 (Market Turnover Analysis)")
    print("="*50)

    if not DATA_PATH:
        print("❌ 错误：找不到 .parquet 数据文件！")
        return

    print(f"📂 正在加载数据: {DATA_PATH}")
    # 只加载时间和代码列，速度极快
    df = pd.read_parquet(DATA_PATH, columns=['Datetime', 'Ticker'])
    
    # 1. 计算全局时间范围 (整个班级的上课时间)
    global_start = df['Datetime'].min()
    global_end = df['Datetime'].max()
    print(f"📅 数据集跨度: {global_start.date()} 至 {global_end.date()}")
    
    # 2. 计算每只股票的生命周期 (每个人的打卡记录)
    print("⚙️ 正在分析 9000+ 只股票的入场与离场时间...")
    lifespans = df.groupby('Ticker')['Datetime'].agg(['min', 'max'])
    
    # 3. 定义判定标准 (容差 Buffer)
    # 如果股票的开始时间比全局开始时间晚 7 天以上，算迟到
    # 如果股票的结束时间比全局结束时间早 7 天以上，算早退
    buffer = pd.Timedelta(days=7)
    
    lifespans['is_late'] = lifespans['min'] > (global_start + buffer)
    lifespans['is_early'] = lifespans['max'] < (global_end - buffer)
    
    # 4. 分类统计
    # A. 全勤 (Full Term): 既没迟到也没早退
    mask_full = (~lifespans['is_late']) & (~lifespans['is_early'])
    
    # B. 迟到 (Late Arrival / IPO): 迟到了，但坚持到了最后
    mask_late = (lifespans['is_late']) & (~lifespans['is_early'])
    
    # C. 早退 (Early Departure / Delist): 一开始就在，但中途走了
    mask_early = (~lifespans['is_late']) & (lifespans['is_early'])
    
    # D. 快闪 (Short-lived): 迟到 + 早退 (中间来了一下又走了)
    mask_flash = (lifespans['is_late']) & (lifespans['is_early'])
    
    count_full = mask_full.sum()
    count_late = mask_late.sum()
    count_early = mask_early.sum()
    count_flash = mask_flash.sum()
    total = len(lifespans)

    # 5. 打印报告
    print("\n📊 统计结果报告:")
    print("-" * 30)
    print(f"🟢 全勤生 (Full Term)    : {count_full} 只 ({count_full/total:.1%})")
    print(f"🟡 迟到组 (New IPOs)    : {count_late} 只 ({count_late/total:.1%}) -> 市场新增血液")
    print(f"🟠 早退组 (Delisted)    : {count_early} 只 ({count_early/total:.1%}) -> 市场淘汰")
    print(f"🔴 快闪组 (Short-lived) : {count_flash} 只 ({count_flash/total:.1%}) -> 昙花一现")
    print("-" * 30)
    print(f"∑ 总计                 : {total} 只")

    # 6. 可视化 (生成饼图)
    print("\n🎨 正在绘制 [图4: 股票生存状态分布]...")
    plt.figure(figsize=(10, 8))
    
    labels = [
        f'Full Term\n({count_full})', 
        f'New Arrivals (IPOs)\n({count_late})', 
        f'Delisted/M&A\n({count_early})', 
        f'Short Lived\n({count_flash})'
    ]
    sizes = [count_full, count_late, count_early, count_flash]
    colors = ['#2ecc71', '#3498db', '#e74c3c', '#95a5a6'] # 绿、蓝、红、灰
    explode = (0.05, 0, 0, 0)  # 突出显示全勤组
    
    plt.pie(sizes, labels=labels, colors=colors, autopct='%1.1f%%', 
            startangle=140, pctdistance=0.85, explode=explode,
            textprops={'fontsize': 14})
    
    # 画个白圈变成甜甜圈图
    centre_circle = plt.Circle((0,0),0.70,fc='white')
    fig = plt.gcf()
    fig.gca().add_artist(centre_circle)
    
    plt.title(f"Market Composition Analysis\n(Total Tickers: {total})", fontsize=16)
    plt.tight_layout()
    
    save_path = os.path.join(OUTPUT_DIR, "4_survivorship_analysis.png")
    plt.savefig(save_path, dpi=300)
    print(f"   --> 已保存: {save_path}")
    
    # 7. (可选) 保存详细名单
    # lifespans.to_csv(os.path.join(OUTPUT_DIR, "turnover_details.csv"))
    # print(f"📝 详细分类名单已保存至 turnover_details.csv")

if __name__ == "__main__":
    analyze_survivorship()