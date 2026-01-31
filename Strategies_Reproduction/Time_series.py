import pandas as pd
import numpy as np
import yfinance as yf
import matplotlib.pyplot as plt
import os

proxy = 'http://127.0.0.1:10808' # 代理设置，此处修改

os.environ['HTTP_PROXY'] = proxy

os.environ['HTTPS_PROXY'] = proxy
def trend_following_strategy(ticker, start_date, end_date, n_fast=20, n_slow=120):
    # 1. 获取数据 (禁用多线程以防卡死)
    print(f"正在下载 {ticker} 数据...")
    df = yf.download(ticker, start=start_date, end=end_date, threads=False, progress=False)
    
    # 修复 yfinance 可能出现的 MultiIndex 问题 (如果有 Ticker 层级则删除)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.droplevel(1)
    
    # 确保没有空值
    df = df.dropna()
    
    # 2. 计算基础指标
    # 计算对数收益率
    df['Log_Return'] = np.log(df['Close'] / df['Close'].shift(1))
    
    # 计算 SMA
    df['SMA_Fast'] = df['Close'].rolling(window=n_fast).mean()
    df['SMA_Slow'] = df['Close'].rolling(window=n_slow).mean()

    # 计算 ATR (用于止损)
    df['H-L'] = df['High'] - df['Low']
    df['H-PC'] = abs(df['High'] - df['Close'].shift(1))
    df['L-PC'] = abs(df['Low'] - df['Close'].shift(1))
    df['TR'] = df[['H-L', 'H-PC', 'L-PC']].max(axis=1)
    df['ATR'] = df['TR'].rolling(window=14).mean()

    # 3. 生成交易信号
    # 初始化
    df['Position'] = 0.0
    # 策略逻辑: 快线 > 慢线 = 做多(1), 否则 = 空仓(0)
    df['Position'] = np.where(df['SMA_Fast'] > df['SMA_Slow'], 1.0, 0.0)
    
    # 4. 执行回测
    # 仓位下移一天 (今天收盘出的信号，明天才能买入)
    df['Strategy_Return'] = df['Position'].shift(1) * df['Log_Return']
    
    # 计算累计净值曲线
    df['Buy_Hold_Curve'] = df['Log_Return'].cumsum().apply(np.exp)
    df['Strategy_Curve'] = df['Strategy_Return'].cumsum().apply(np.exp)
    
    return df

# --- 主程序 ---
if __name__ == "__main__":
    ticker = 'SPY'
    start_date = '2018-01-01'
    end_date = '2023-12-31'

    try:
        # 运行策略
        result = trend_following_strategy(ticker, start_date, end_date)

        # --- 绘图与保存 ---
        print("正在绘图...")
        plt.figure(figsize=(14, 8))

        # 子图 1: 策略收益 vs 买入持有
        plt.subplot(2, 1, 1)
        plt.plot(result.index, result['Strategy_Curve'], label='Trend Following Strategy', color='orange')
        plt.plot(result.index, result['Buy_Hold_Curve'], label='Buy & Hold (Benchmark)', color='gray', alpha=0.5)
        plt.title(f'Strategy Performance: SMA(20) vs SMA(120) on {ticker}')
        plt.legend()
        plt.grid(True)

        # 子图 2: 价格与信号
        plt.subplot(2, 1, 2)
        # 只取最后 500 天数据展示细节
        subset = result.iloc[-500:] 
        plt.plot(subset.index, subset['Close'], label='Price', alpha=0.3, color='black')
        plt.plot(subset.index, subset['SMA_Fast'], label='SMA 20 (Fast)', color='blue')
        plt.plot(subset.index, subset['SMA_Slow'], label='SMA 120 (Slow)', color='red')

        # 填充信号区域
        fill_condition = subset['SMA_Fast'] > subset['SMA_Slow']
        plt.fill_between(subset.index, subset['SMA_Fast'], subset['SMA_Slow'], 
                        where=fill_condition, color='green', alpha=0.1, label='Long Zone')
        plt.fill_between(subset.index, subset['SMA_Fast'], subset['SMA_Slow'], 
                        where=~fill_condition, color='red', alpha=0.1, label='Neutral Zone')

        plt.title('Signals: Golden Cross vs Death Cross')
        plt.legend()
        plt.grid(True)
        plt.tight_layout()

        # 关键修改：保存图片到脚本所在目录，而不是根目录
        current_dir = os.path.dirname(os.path.abspath(__file__))
        save_path = os.path.join(current_dir, 'strategy_result.png')
        
        plt.savefig(save_path)
        print(f"✅ 图片已保存为: {save_path}")
        
        # 尝试显示图片 (如果在终端不支持显示也不会报错)
        try:
            plt.show()
        except:
            pass

        # --- 打印结果 (关键修复: 增加 .item() 转换格式) ---
        # iloc[-1] 取最后一行，如果是 Series 依然会报错，必须转为 float
        latest_price = float(result['Close'].iloc[-1])
        latest_atr = float(result['ATR'].iloc[-1])

        print("-" * 30)
        print(f"当前价格: {latest_price:.2f}")
        print(f"当前 ATR(14): {latest_atr:.2f}")
        print(f"建议止损位 (Price - 2*ATR): {latest_price - 2*latest_atr:.2f}")
        print("-" * 30)

    except Exception as e:
        print(f"❌ 发生错误: {e}")