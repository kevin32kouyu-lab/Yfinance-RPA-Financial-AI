import pandas as pd
import os
import argparse
from datetime import datetime, time
import pytz

# ==================== 1. 配置区域 ====================
current_script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_script_dir)

# 输入文件：特征数据库
DATA_FILE = os.path.join(project_root, "data_process", "output", "engineered_features_final.parquet")
# 输出目录：严格指定为 data_process/output
EXPORT_DIR = os.path.join(project_root, "data_process", "output")

if not os.path.exists(EXPORT_DIR):
    os.makedirs(EXPORT_DIR)

# ==================== 2. 核心查询类 (Pro版) ====================
class USStockQueryTool:
    def __init__(self, file_path):
        print(f"📂 [美股精细化查询] 正在加载数据库: {file_path} ...")
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"找不到特征数据库: {file_path}")
        
        self.df = pd.read_parquet(file_path)
        print(f"✅ 数据库加载完成！共 {len(self.df):,} 条记录。")
        
        # 强制转换为美东时间
        if self.df['Datetime'].dt.tz is None:
             self.df['Datetime'] = self.df['Datetime'].dt.tz_localize('UTC').dt.tz_convert('America/New_York')
        else:
             self.df['Datetime'] = self.df['Datetime'].dt.tz_convert('America/New_York')

    def parse_input_time(self, date_str, is_end_time=False):
        """
        智能解析时间字符串。
        - 输入 "YYYY-MM-DD" -> 自动补充为 09:30 (开始) 或 16:00 (结束)
        - 输入 "YYYY-MM-DD HH:MM" -> 保持精确时间
        """
        date_str = date_str.strip()
        ny_tz = pytz.timezone('America/New_York')
        
        # 尝试格式 1: 仅日期 (YYYY-MM-DD)
        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d")
            # 默认补全逻辑
            if is_end_time:
                # 结束时间默认设为 16:00:00 (美股收盘)
                dt = dt.replace(hour=16, minute=0, second=0)
            else:
                # 开始时间默认设为 09:30:00 (美股开盘)
                dt = dt.replace(hour=9, minute=30, second=0)
            return ny_tz.localize(dt), True # True 表示使用了默认补全
        except ValueError:
            pass

        # 尝试格式 2: 精确到分 (YYYY-MM-DD HH:MM)
        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M")
            return ny_tz.localize(dt), False
        except ValueError:
            pass
            
        # 尝试格式 3: 精确到秒 (YYYY-MM-DD HH:MM:SS)
        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
            return ny_tz.localize(dt), False
        except ValueError:
            pass

        return None, False

    def query(self, ticker, start_str, end_str):
        ticker = ticker.strip().upper()
        if ticker == "VIX": ticker = "^VIX"

        # 1. 解析时间
        start_date, is_default_start = self.parse_input_time(start_str, is_end_time=False)
        end_date, is_default_end = self.parse_input_time(end_str, is_end_time=True)

        if not start_date or not end_date:
            print("❌ 时间格式无法识别！支持格式：'2025-01-01' 或 '2025-01-01 14:30'")
            return

        print(f"\n🇺🇸 正在查询: [{ticker}]")
        print(f"   📅 时间范围: {start_date} 至 {end_date}")
        if is_default_start and is_default_end:
            print("   ℹ️ (已自动应用美股交易时段: 09:30 - 16:00)")

        # 2. 筛选数据
        stock_df = self.df[self.df['Ticker'] == ticker].copy()
        if stock_df.empty:
            print(f"❌ 数据库中没有 {ticker} 的记录。")
            return

        mask = (stock_df['Datetime'] >= start_date) & (stock_df['Datetime'] <= end_date)
        result_df = stock_df.loc[mask]

        if result_df.empty:
            print("❌ 该时间段内无数据。")
            return

        # 3. 输出与保存
        print("\n" + "="*80)
        print(f"📊 查询结果: {len(result_df)} 条记录")
        print("="*80)
        
        base_cols = ['Datetime', 'Open', 'High', 'Low', 'Close', 'Volume']
        tech_cols = [c for c in result_df.columns if c not in base_cols and c != 'Ticker']
        
        # 检查 NaN
        nan_cols = result_df[tech_cols].isnull().sum()
        if nan_cols.sum() > 0:
            print("\n⚠️ 注意: 以下指标存在空值 (通常因历史数据不足):")
            for col, count in nan_cols.items():
                if count > 0: print(f"   - {col}: 缺 {count}")

        # 预览
        display_cols = base_cols + tech_cols
        pd.set_option('display.max_columns', None)
        print("\n数据预览 (首尾各 2 行):")
        if len(result_df) > 4:
            print(result_df[display_cols].iloc[[0, 1, -2, -1]].to_string(index=False))
        else:
            print(result_df[display_cols].to_string(index=False))
        
        # 导出到指定目录
        safe_start = start_str.replace(":", "").replace(" ", "_")
        safe_end = end_str.replace(":", "").replace(" ", "_")
        save_name = f"Query_{ticker}_{safe_start}_to_{safe_end}.csv"
        save_path = os.path.join(EXPORT_DIR, save_name)
        
        result_df[display_cols].to_csv(save_path, index=False)
        print(f"\n💾 文件已成功保存至:\n   👉 {save_path}")

# ==================== 3. 交互入口 ====================
if __name__ == "__main__":
    tool = USStockQueryTool(DATA_FILE)
    
    while True:
        print("\n" + "-"*50)
        print("🇺🇸 美股指标精细查询器 (q=退出)")
        print("💡 提示: 输入 '2025-01-01' 默认查 09:30-16:00")
        print("        输入 '2025-01-01 13:00' 可精确查下午盘")
        print("-"*50)
        
        ticker = input("代码 (如 AAPL): ").strip()
        if ticker.lower() == 'q': break
            
        start = input("开始时间: ").strip()
        end = input("结束时间: ").strip()
        
        tool.query(ticker, start, end)