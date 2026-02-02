import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
import warnings

warnings.filterwarnings('ignore')

# --- 动态路径配置 (核心修复) ---
# 获取当前脚本所在目录 (data_process)
current_script_dir = os.path.dirname(os.path.abspath(__file__))
# 获取项目根目录 (Yfinance RPA) - 假设脚本在子文件夹中，向上一级
project_root = os.path.dirname(current_script_dir)

# 如果脚本直接放在根目录下，上面的 project_root 可能会跑偏
# 所以做一个简单的检查：如果 output 文件夹不在计算出的 root 下，就尝试当前目录
if not os.path.exists(os.path.join(project_root, "output")):
    if os.path.exists(os.path.join(current_script_dir, "output")):
        project_root = current_script_dir
    else:
        # 最后的保底：假设脚本在 data_process，尝试硬编码回退
        # 你的路径结构是 E:/Msc project/Yfinance RPA/data_process/verify_data.py
        # 所以 project_root 应该是 E:/Msc project/Yfinance RPA
        pass

OUTPUT_FILE = os.path.join(project_root, "output", "engineered_features.parquet")
PLOT_DIR = os.path.join(project_root, "output")

def verify_data():
    print("="*50)
    print("🔍 交互式数据验证工具 (Interactive Validator)")
    print(f"📂 目标路径: {OUTPUT_FILE}")
    print("="*50)

    if not os.path.exists(OUTPUT_FILE):
        print(f"❌ 严重错误：找不到特征文件！")
        print(f"   期待路径: {OUTPUT_FILE}")
        print("   请确认 feature_engineering.py 是否成功运行并保存到了 output 文件夹。")
        return

    print("📂 正在加载特征数据 (请稍候)...")
    try:
        df = pd.read_parquet(OUTPUT_FILE)
    except Exception as e:
        print(f"❌ 读取失败: {e}")
        return
    
    # 确保 Ticker 是列
    if 'Ticker' not in df.columns:
        print("⚠️ 检测到 Ticker 不在列中，尝试重置索引...")
        df = df.reset_index()

    available_tickers = df['Ticker'].unique()
    count = len(available_tickers)
    print(f"✅ 数据加载完毕！共包含 {count} 只股票。")
    print(f"📋 示例代码: {available_tickers[:10]} ...")
    print("-" * 30)

    # --- 交互式输入 ---
    while True:
        user_input = input("\n👉 请输入你要验证的 Ticker (输入 q 退出): ").strip().upper()
        
        if user_input == 'Q':
            print("👋 退出验证。")
            break
        
        if user_input not in available_tickers:
            print(f"❌ 错误：数据集中找不到股票代码 [{user_input}]，请重新输入。")
            continue

        # --- 开始针对该股票验证 ---
        print(f"\n🧪 正在验证 [{user_input}] ...")
        
        # 提取该股票数据
        ticker_df = df[df['Ticker'] == user_input].sort_values('Datetime').copy()
        
        # 1. 基础检查
        print(f"   - 数据行数: {len(ticker_df)} 行")
        if len(ticker_df) == 0:
            print("   ❌ 数据为空！")
            continue
            
        # 2. 均线计算验证 (Manual Check)
        manual_sma = ticker_df['Close'].rolling(20).mean()
        # 计算与文件中 SMA_20 的差异 (忽略前20个NaN)
        # 注意：文件里的 SMA_20 已经是计算好的，且前50行已经被 feature_engineering 清洗过
        # 所以我们直接对比两者的有效部分
        
        # 对齐索引进行相减
        diff = (ticker_df['SMA_20'] - manual_sma).dropna()
        
        if diff.empty:
             print("   ⚠️ 数据太短或无法对齐，无法验证 SMA。")
        else:
            max_diff = diff.abs().max()
            if max_diff < 1e-4:
                print(f"   ✅ SMA_20 计算正确 (最大误差: {max_diff:.8f})")
            else:
                print(f"   ❌ SMA_20 计算异常 (最大误差: {max_diff})")

        # 3. 逻辑检查
        rsi_check = ticker_df['RSI_14'].between(0, 100).all()
        print(f"   - RSI 范围 (0-100): {'✅ 正常' if rsi_check else '❌ 异常'}")
        
        # 4. 绘图验证
        try:
            print(f"   📈 正在生成验证图表...")
            # 只画最后 300 个点
            plot_df = ticker_df.iloc[-300:] 
            
            plt.figure(figsize=(12, 6))
            plt.plot(plot_df['Datetime'], plot_df['Close'], label='Close', color='black', alpha=0.6)
            plt.plot(plot_df['Datetime'], plot_df['SMA_20'], label='SMA 20', color='orange', linewidth=1.5)
            plt.plot(plot_df['Datetime'], plot_df['SMA_50'], label='SMA 50', color='green', linewidth=1.5)
            
            plt.title(f"Validation: {user_input} (Last 300 Hours)")
            plt.legend()
            plt.grid(True, alpha=0.3)
            
            save_path = os.path.join(PLOT_DIR, f"validation_{user_input}.png")
            plt.savefig(save_path)
            plt.close()
            print(f"   🖼️  验证图表已保存至: {save_path}")
            
        except Exception as e:
            print(f"   ❌ 绘图失败: {e}")

if __name__ == "__main__":
    verify_data()