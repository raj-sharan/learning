
def moving_average_high_9sma(df):
    return df['high'].rolling(window=9).mean().round(2)

def moving_average_low_9sma(df):
    return df['low'].rolling(window=9).mean().round(2)

def moving_average_close_9sma(df):
    return df['close'].rolling(window=9).mean().round(2)
    
def moving_average_close_15sma(df):
    return df['close'].rolling(window=15).mean().round(2)
    
def moving_average_close_20sma(df):
    return df['close'].rolling(window=20).mean().round(2)

def moving_average_close_50sma(df):
    return df['close'].rolling(window=50).mean().round(2)

def moving_average_close_200sma(df):
    return df['close'].rolling(window=200).mean().round(2)

def atr(df, period = 14):
    # Calculate True Range (TR)
    df['previous_close'] = df['close'].shift(1)
    df['high_low'] = df['high'] - df['low']
    df['high_close'] = abs(df['high'] - df['previous_close'])
    df['low_close'] = abs(df['low'] - df['previous_close'])
    
    tr = df[['high_low', 'high_close', 'low_close']].max(axis=1)
    # df['ATR'] = df['TR'].rolling(window=atr_period).mean().round(2)
    return calculate_rma(tr, period)
    
    # For EMA ATR (preferred)
    # df['ATR_EMA'] = df['TR'].ewm(span=atr_period, adjust=False).mean().round(2)
    
    
def calculate_rma(data, period):
    rma_values = []
    
    # First RMA value is the SMA of the first 'period' values
    first_rma = data[:period].mean()
    rma_values.append(first_rma.round(2))
    
    # Calculate RMA for the rest
    for price in data[period:]:
        prev_rma = rma_values[-1]
        rma = ((prev_rma * (period - 1)) + price) / period
        rma_values.append(rma.round(2))
    
    # Fill the first (period-1) values with NaN for alignment
    return [None] * (period - 1) + rma_values

    
    