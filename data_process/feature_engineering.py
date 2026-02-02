import pandas as pd
import numpy as np
import os
import warnings
from tqdm import tqdm

# 忽略计算过程中可能出现的除零警告
warnings.filterwarnings('ignore')

# ==================== 1. 路径配置 ====================
current_script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_script_dir)

# 输入文件：清洗合并后的统一市场数据
INPUT_FILE = os.path.join(project_root, "data_process", "full_market_data.parquet")
# 输出配置
OUTPUT_DIR = os.path.join(project_root, "data_process", "output")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "engineered_features_final.parquet")

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

# ==================== 2. 核心特征计算函数 (带详细备注) ====================
def compute_technical_indicators(df):
    """
    为单只股票计算全套技术指标。
    输入: 包含 OHLCV 的 DataFrame (必须包含 Datetime, Open, High, Low, Close, Volume)
    """
    # [数据预处理] 必须按时间排序，否则滑窗计算(Rolling)会错乱
    df = df.sort_values('Datetime')
    
    # 防止除以零的微小常数
    epsilon = 1e-9

    # -------------------------------------------------------
    # A. 基础收益与风险 (Ref: PDF Page 2)
    # -------------------------------------------------------
    # 1. Log Return (对数收益率): l_t = ln(P_t / P_{t-1})
    # 作用: 具有可加性，分布更正态，AI 模型首选。
    df['Log_Return'] = np.log(df['Close'] / df['Close'].shift(1))
    
    # 2. Historical Volatility (历史波动率): std(Log_Return, 20)
    # 作用: 衡量过去 20 天的风险/不确定性。
    df['Vol_20'] = df['Log_Return'].rolling(window=20).std()

    # -------------------------------------------------------
    # B. 趋势指标 (Trend - Ref: PDF Page 2 SMA/EMA)
    # -------------------------------------------------------
    # 3. SMA (简单移动平均): 20日(短期) 和 50日(中期)
    df['SMA_20'] = df['Close'].rolling(window=20).mean()
    df['SMA_50'] = df['Close'].rolling(window=50).mean()
    
    # 4. EMA (指数移动平均): 对近期价格权重更高
    df['EMA_12'] = df['Close'].ewm(span=12, adjust=False).mean()
    df['EMA_26'] = df['Close'].ewm(span=26, adjust=False).mean()
    
    # 5. Bias (乖离率): (Price - SMA) / SMA
    # 作用: 衡量价格偏离均线的程度。正值过大=超买，负值过大=超卖。
    df['Bias_20'] = (df['Close'] - df['SMA_20']) / (df['SMA_20'] + epsilon)

    # -------------------------------------------------------
    # C. 动量指标 (Momentum - Ref: PDF Page 13 Feature Vector)
    # -------------------------------------------------------
    # 6. RSI (相对强弱指数): 衡量多空力量对比 (0-100)
    delta = df['Close'].diff()
    gain = (delta.where(delta > 0, 0))
    loss = (-delta.where(delta < 0, 0))
    # 使用 Wilder 平滑法计算平均涨跌
    avg_gain = gain.ewm(alpha=1/14, min_periods=14, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/14, min_periods=14, adjust=False).mean()
    rs = avg_gain / (avg_loss + epsilon)
    df['RSI_14'] = 100 - (100 / (1 + rs))

    # 7. MACD (异同移动平均): 趋势+动量的双重指标
    # DIF (快线) = EMA_12 - EMA_26
    df['MACD'] = df['EMA_12'] - df['EMA_26']
    # DEA (信号线) = DIF 的 9日 EMA
    df['MACD_Signal'] = df['MACD'].ewm(span=9, adjust=False).mean()
    # Histogram (能量柱) = DIF - DEA (正值代表多头主导)
    df['MACD_Hist'] = df['MACD'] - df['MACD_Signal']

    # 8. ROC (变动率): (P_t - P_{t-n}) / P_{t-n}
    # 作用: 纯粹的价格动量速度。
    df['ROC_10'] = df['Close'].pct_change(periods=10) * 100

    # -------------------------------------------------------
    # D. 波动通道指标 (Volatility Channels)
    # -------------------------------------------------------
    # 9. Bollinger Bands (布林带): SMA_20 +/- 2倍标准差
    bb_mid = df['Close'].rolling(window=20).mean()
    bb_std = df['Close'].rolling(window=20).std()
    
    df['BB_Upper'] = bb_mid + 2 * bb_std
    df['BB_Lower'] = bb_mid - 2 * bb_std
    # %B 指标: 价格在布林带中的相对位置 (>1 突破上轨, <0 跌破下轨)
    df['BB_PctB'] = (df['Close'] - df['BB_Lower']) / (df['BB_Upper'] - df['BB_Lower'] + epsilon)
    # Band Width: 带宽，衡量波动率挤压 (Squeeze)
    df['BB_Width'] = (df['BB_Upper'] - df['BB_Lower']) / (bb_mid + epsilon)

    # 10. ATR (真实波幅): 衡量日内波动的绝对值
    # TR = max(H-L, |H-PreClose|, |L-PreClose|)
    prev_close = df['Close'].shift(1)
    tr1 = df['High'] - df['Low']
    tr2 = (df['High'] - prev_close).abs()
    tr3 = (df['Low'] - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    df['ATR_14'] = tr.rolling(window=14).mean()

    # -------------------------------------------------------
    # E. 振荡指标 (Oscillators)
    # -------------------------------------------------------
    # 11. Stochastic (KDJ的K和D): 价格在 N 天极值范围内的位置
    low_14 = df['Low'].rolling(window=14).min()
    high_14 = df['High'].rolling(window=14).max()
    
    # %K 线
    df['Stoch_K'] = 100 * ((df['Close'] - low_14) / (high_14 - low_14 + epsilon))
    # %D 线 (%K 的 3日均线)
    df['Stoch_D'] = df['Stoch_K'].rolling(window=3).mean()

    # -------------------------------------------------------
    # F. 成交量特征 (Volume)
    # -------------------------------------------------------
    # 12. Volume Change (量比变化)
    df['Vol_Change'] = df['Volume'].pct_change().fillna(0).replace([np.inf, -np.inf], 0)
    
    # 13. OBV (能量潮): 价格涨累加成交量，价格跌减去成交量
    obv_direction = np.where(df['Close'] > df['Close'].shift(1), 1, -1)
    obv_direction[0] = 0 
    df['OBV'] = (obv_direction * df['Volume']).cumsum()

    # -------------------------------------------------------
    # G. 滞后特征 (Lagged Features - Ref: PDF Labels)
    # -------------------------------------------------------
    # 14. Lags (滞后项): 让模型“看到”过去几天的状态
    df['Log_Return_Lag1'] = df['Log_Return'].shift(1) # 昨天的收益
    df['Log_Return_Lag2'] = df['Log_Return'].shift(2) # 前天的收益
    df['Log_Return_Lag3'] = df['Log_Return'].shift(3) 

    return df

# ==================== 3. 主程序 (手动循环版) ====================
def run_feature_engineering():
    print("="*50)
    print("🚀 高级特征工程启动 (Annotated Version)")
    print("="*50)
    
    if not os.path.exists(INPUT_FILE):
        print(f"❌ 找不到输入文件: {INPUT_FILE}")
        return

    print(f"📂 正在读取原始数据: {INPUT_FILE}")
    df = pd.read_parquet(INPUT_FILE)
    print(f"📊 原始数据量: {len(df):,} 行 | 股票数: {df['Ticker'].nunique()}")

    print("\n⚙️ 正在计算特征 (使用手动循环，稳定无报错)...")
    
    # 弃用 progress_apply，改用原生循环以避免 pandas 版本冲突
    grouped = df.groupby('Ticker')
    results = []
    
    for ticker, group in tqdm(grouped, desc="Processing Tickers"):
        res = compute_technical_indicators(group)
        results.append(res)
    
    print("\n📦 正在合并结果...")
    df_engineered = pd.concat(results)

    # 清洗预热期的空值 (因为计算MA50需要前50天数据)
    print("🧹 清洗空值 (Dropna)...")
    original_len = len(df_engineered)
    df_engineered.dropna(inplace=True)
    print(f"   - 删除行数: {original_len - len(df_engineered)} (预热期数据)")

    # 索引重置
    df_engineered.reset_index(drop=True, inplace=True)

    # 完整性检查
    required_cols = ['Ticker', 'Datetime', 'Close', 'RSI_14', 'BB_PctB']
    if all(col in df_engineered.columns for col in required_cols):
        print("✅ 特征完整性检查通过。")
    else:
        print(f"❌ 警告: 缺失列 -> {[c for c in required_cols if c not in df_engineered.columns]}")

    print(f"\n💾 保存至: {OUTPUT_FILE}")
    df_engineered.to_parquet(OUTPUT_FILE, engine='pyarrow', compression='snappy')
    
    print("="*50)
    print("✨ 完成！前 3 行预览:")
    cols_to_show = ['Datetime', 'Ticker', 'Close', 'RSI_14', 'BB_PctB']
    print(df_engineered[cols_to_show].head(3).to_string())

if __name__ == "__main__":
    run_feature_engineering()