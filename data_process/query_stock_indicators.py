import pandas as pd
import os
import argparse
from datetime import datetime
import pytz

# ==================== 1. 配置区域 ====================
current_script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_script_dir)

# 指向生成的特征文件 (假设你已经运行了特征工程脚本)
# 如果你的美股特征文件名字不同，请在这里修改
DATA_FILE = os.path.join(project_root, "data_process", "output", "engineered_features_final.parquet")

# ==================== 2. 核心查询类 (美股版) ====================
class USStockQueryTool:
    def __init__(self, file_path):
        print(f"📂 [美股模式] 正在加载数据库: {file_path} ...")
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"找不到特征数据库: {file_path}\n请确保你已经针对美股数据运行了 feature_engineering.py！")
        
        self.df = pd.read_parquet(file_path)
        print(f"✅ 数据库加载完成！共 {len(self.df):,} 条记录。")
        
        # 核心修改：强制转换为美东时间 (EST/EDT)
        # 这样你在查询 '2025-01-01' 时，对应的是纽约的早上，而不是北京的早上
        if self.df['Datetime'].dt.tz is None:
             self.df['Datetime'] = self.df['Datetime'].dt.tz_localize('UTC').dt.tz_convert('America/New_York')
        else:
             self.df['Datetime'] = self.df['Datetime'].dt.tz_convert('America/New_York')

    def query(self, ticker, start_str, end_str):
        # 1. 格式化 Ticker (美股处理逻辑)
        ticker = ticker.strip().upper()
        
        # 美股通常不需要加后缀，除非是特定格式
        # 自动处理常见输入错误：比如把 'vix' 输成小写，或者忘了加 '=f'
        if ticker == "VIX": ticker = "^VIX" # 自动修正恐慌指数
        
        print(f"\n🇺🇸 正在查询: [{ticker}] (美东时间) {start_str} 至 {end_str}")

        # 2. 筛选 Ticker
        stock_df = self.df[self.df['Ticker'] == ticker].copy()
        
        if stock_df.empty:
            print(f"❌ 未找到代码为 {ticker} 的数据。")
            print("   提示：美股代码直接输入即可 (如 AAPL, SPY)。期货请带后缀 (如 ES=F, CL=F)。")
            return

        # 3. 检查数据有效性范围
        min_date = stock_df['Datetime'].min()
        max_date = stock_df['Datetime'].max()
        
        print(f"ℹ️ 数据有效覆盖期: {min_date.strftime('%Y-%m-%d')} 至 {max_date.strftime('%Y-%m-%d')}")

        # 4. 时间过滤 (使用美东时间)
        ny_tz = pytz.timezone('America/New_York')
        try:
            # 构造查询区间的开始和结束
            start_date = ny_tz.localize(datetime.strptime(start_str, "%Y-%m-%d"))
            end_date = ny_tz.localize(datetime.strptime(end_str + " 23:59:59", "%Y-%m-%d %H:%M:%S"))
        except ValueError:
            print("❌ 日期格式错误！请使用 YYYY-MM-DD 格式。")
            return

        if start_date < min_date:
            print(f"⚠️ 警告: 开始时间早于数据起点。前 50 个周期可能因指标预热而被剔除。")

        mask = (stock_df['Datetime'] >= start_date) & (stock_df['Datetime'] <= end_date)
        result_df = stock_df.loc[mask]

        if result_df.empty:
            print("❌ 该时间段内无数据。请确认美股在该日期是否开盘（留意周末和节假日）。")
            return

        # 5. 智能输出
        print("\n" + "="*80)
        print(f"📊 查询结果: {ticker} ({len(result_df)} 行)")
        print("="*80)
        
        base_cols = ['Datetime', 'Open', 'High', 'Low', 'Close', 'Volume']
        # 动态抓取所有计算出来的技术指标列
        tech_cols = [c for c in result_df.columns if c not in base_cols and c != 'Ticker']
        
        # 检查 NaN 并解释原因
        nan_cols = result_df[tech_cols].isnull().sum()
        if nan_cols.sum() > 0:
            print("\n⚠️ 部分指标不可用 (N/A):")
            for col, count in nan_cols.items():
                if count > 0:
                    reason = "历史数据不足 (预热期)" if "SMA" in col or "Vol" in col else "计算依赖前序数据"
                    print(f"   - {col}: 缺 {count} 个 ({reason})")

        # 打印预览
        display_cols = base_cols + tech_cols
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', 1000)
        
        print("\n数据预览 (最新 5 行):")
        # 按时间正序打印
        print(result_df[display_cols].tail(5).to_string(index=False))
        
        # 导出
        save_name = f"US_Query_{ticker}_{start_str}_{end_str}.csv"
        result_df[display_cols].to_csv(save_name, index=False)
        print(f"\n💾 文件已导出: {save_name}")

# ==================== 3. 交互入口 ====================
if __name__ == "__main__":
    # 自动定位文件路径
    tool = USStockQueryTool(DATA_FILE)
    
    while True:
        print("\n" + "-"*40)
        print("🇺🇸 美股/期货特征查询器 (输入 q 退出)")
        print("-"*40)
        ticker_input = input("请输入代码 (如 AAPL, SPY, ES=F): ").strip()
        if ticker_input.lower() == 'q': break
            
        start_input = input("开始日期 (YYYY-MM-DD): ").strip()
        end_input = input("结束日期 (YYYY-MM-DD): ").strip()
        
        tool.query(ticker_input, start_input, end_input)