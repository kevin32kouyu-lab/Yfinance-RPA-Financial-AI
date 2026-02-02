import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
import os
import numpy as np

# --- 自动寻找数据文件 ---
current_dir = os.path.dirname(os.path.abspath(__file__))

# 项目根目录（visualization 在项目子目录里）
PROJECT_ROOT = os.path.abspath(os.path.join(current_dir, ".."))
DATA_PROCESS_DIR = os.path.join(PROJECT_ROOT, "data_process")
OUTPUT_ROOT = os.path.join(PROJECT_ROOT, "output")

# 优先顺序：output/engineered_features -> data_process/full_market_data -> output/full_market_data
possible_paths = [
    os.path.join(OUTPUT_ROOT, "engineered_features.parquet"),
    os.path.join(DATA_PROCESS_DIR, "full_market_data.parquet"),
    os.path.join(OUTPUT_ROOT, "full_market_data.parquet"),
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

OUTPUT_IMG_DIR = os.path.join(current_dir, "output", "report_images")
if not os.path.exists(OUTPUT_IMG_DIR):
    os.makedirs(OUTPUT_IMG_DIR)

# 设置绘图风格
plt.style.use('ggplot')
sns.set_context("notebook", font_scale=1.2)
plt.rcParams['font.sans-serif'] = ['Arial'] # 防止中文乱码兼容性问题，用英文通用字体

def run_visualization():
    print("="*50)
    print("📊 数据集可视化生成器 (Report Generator)")
    print("="*50)

    if not DATA_PATH:
        print("❌ 错误：找不到 .parquet 数据文件！请确认路径。")
        return

    print(f"📂 正在加载数据: {DATA_PATH}")
    print("   (文件较大，请耐心等待几秒...)")
    
    # 只加载必要的列，节省内存
    df = pd.read_parquet(DATA_PATH, columns=['Datetime', 'Ticker', 'Close'])
    
    total_tickers = df['Ticker'].nunique()
    total_rows = len(df)
    min_date = df['Datetime'].min()
    max_date = df['Datetime'].max()
    
    print(f"✅ 数据加载完成！共 {total_rows:,} 行，{total_tickers} 只股票。")

    # ========================================================
    # 图表 1: 市场活跃度曲线 (Market Breadth)
    # 解释：展示每个小时究竟有多少只股票在交易，打破“数字打架”的误区
    # ========================================================
    print("\n📈 正在绘制 [图1: 市场活跃度曲线]...")
    plt.figure(figsize=(12, 6))
    
    # 按时间分组计数
    active_counts = df.groupby('Datetime')['Ticker'].count()
    
    # 绘图
    plt.plot(active_counts.index, active_counts.values, color='#2980b9', linewidth=1)
    
    # 标注平均值
    mean_count = active_counts.mean()
    plt.axhline(mean_count, color='#e74c3c', linestyle='--', label=f'Average: {int(mean_count)}')
    
    plt.title(f'Market Breadth: Active Stocks per Hour\n(Total Union Tickers: {total_tickers})', fontsize=16)
    plt.xlabel('Date')
    plt.ylabel('Number of Active Tickers')
    plt.legend()
    plt.grid(True, alpha=0.3)
    
    save_path1 = os.path.join(OUTPUT_IMG_DIR, "1_market_breadth.png")
    plt.savefig(save_path1, dpi=300, bbox_inches='tight')
    print(f"   --> 已保存: {save_path1}")

    # ========================================================
    # 图表 2: 随机抽样价格走势 (Data Integrity Check)
    # 解释：随机抽 20 只股票画在一张图上，证明数据是连续的，不是断断续续的
    # ========================================================
    print("🍝 正在绘制 [图2: 价格走势抽样]...")
    plt.figure(figsize=(12, 6))
    
    # 随机抽 20 个 Ticker
    sample_tickers = np.random.choice(df['Ticker'].unique(), 20, replace=False)
    subset = df[df['Ticker'].isin(sample_tickers)].copy()
    
    # Pivot 表格以便绘图
    pivot_df = subset.pivot(index='Datetime', columns='Ticker', values='Close')
    
    # 归一化：全部除以第一天的价格，起跑线设为 1.0
    normalized_df = pivot_df / pivot_df.bfill().iloc[0]
    
    plt.plot(normalized_df.index, normalized_df.values, alpha=0.6, linewidth=1.5)
    
    plt.title('Sample Price Movements (Normalized, 20 Random Stocks)', fontsize=16)
    plt.xlabel('Date')
    plt.ylabel('Normalized Price (Start = 1.0)')
    plt.grid(True, alpha=0.3)
    
    save_path2 = os.path.join(OUTPUT_IMG_DIR, "2_price_samples.png")
    plt.savefig(save_path2, dpi=300, bbox_inches='tight')
    print(f"   --> 已保存: {save_path2}")

    # ========================================================
    # 图表 3: 数据集概览卡片 (Summary Card)
    # 解释：直接生成一张包含所有关键数据的图片，适合放在 PPT 首页
    # ========================================================
    print("📝 正在生成 [图3: 数据集概览卡片]...")
    
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.axis('off')
    
    # 构造显示的文本
    summary_text = (
        f"DATASET SUMMARY REPORT\n"
        f"--------------------------------\n\n"
        f"Dataset Name   : US Stock Market (Hourly)\n"
        f"Total Records  : {total_rows:,}\n"
        f"Total Tickers  : {total_tickers:,} (Union)\n"
        f"Avg Active/Hr  : {int(mean_count):,} (Snapshot)\n"
        f"Time Range     : {min_date.date()} to {max_date.date()}\n"
        f"Data Frequency : 1-Hour (60min)\n"
        f"Columns        : OHLCV + Ticker\n\n"
        f"Status         : Cleaned & Aligned"
    )
    
    # 在画布中心写字
    ax.text(0.5, 0.5, summary_text, 
            fontsize=14, 
            family='monospace', 
            ha='center', va='center',
            bbox=dict(boxstyle="round,pad=1", facecolor="#fdfefe", edgecolor="#bdc3c7", linewidth=2))
    
    save_path3 = os.path.join(OUTPUT_IMG_DIR, "3_summary_card.png")
    plt.savefig(save_path3, dpi=300, bbox_inches='tight')
    print(f"   --> 已保存: {save_path3}")
    
    print("\n✨ 全部完成！请打开 output/report_images 文件夹查看图片。")

if __name__ == "__main__":
    run_visualization()