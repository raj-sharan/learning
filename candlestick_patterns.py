from datetime import datetime

def single_shooting_star(index, data):
    candle      = data.iloc[index]
    
    close = candle["close"]
    open  = candle["open"]
    high  = candle["high"]
    low   = candle["low"]

    candle_length = high - low > 15.0
    body = open - close

    return (close < open and candle_length and body > 5.0 and
            high - open > body * 2.0 and
            close - low <= body * 0.2)
    
def is_shooting_star(index, data):
    candle      = data.iloc[index]
    prev_candle = data.iloc[index-1]
    
    close = candle["close"]
    open  = candle["open"]
    high  = candle["high"]
    low   = candle["low"]

    prev_close = prev_candle["close"]
    prev_open  = prev_candle["open"]
    prev_high  = prev_candle["high"]
    prev_low   = prev_candle["low"]

    candle_length = high - low > 10
    pre_candle_body = prev_close - prev_open > 5

    return (close < open and candle_length and pre_candle_body and low > prev_open + 1.0 and
            high - max(open, close) >= abs(open - close) * 1.5 and
            min(close, open) - low <= abs(open - close))


def single_hammer(index, data):
    candle      = data.iloc[index]
    
    close = candle["close"]
    open  = candle["open"]
    high  = candle["high"]
    low   = candle["low"]

    candle_length = high - low > 15.0
    body = close - open

    return (close > open and candle_length and body > 5.0 and
            open - low > body * 2 and
            high - close <= body * 0.2)
            
def is_hammer(index, data):
    candle = data.iloc[index]
    prev_candle = data.iloc[index-1]
    prev_candle = data.iloc[index-1]
    
    close = candle["close"]
    open  = candle["open"]
    high  = candle["high"]
    low   = candle["low"]

    prev_close = prev_candle["close"]
    prev_open  = prev_candle["open"]
    prev_high  = prev_candle["high"]
    prev_low   = prev_candle["low"]

    candle_length = high - low > 10
    pre_candle_body = prev_open - prev_close  > 5

    return (close > open and candle_length and pre_candle_body and prev_open > high + 1.0 and
            min(open, close) - low >= abs(open - close) * 1.5 and 
            high - max(close, open) <= abs(open - close))
            

def is_bearish_engulfing(index, data):
    candle      = data.iloc[index]
    prev_candle = data.iloc[index-1]
    prev_candle_2 = data.iloc[index-2]
    
    close = candle["close"]
    open  = candle["open"]
    high  = candle["high"]
    low   = candle["low"]

    valid_body = open - close >= 7

    prev_close = prev_candle["close"]
    prev_open  = prev_candle["open"]
    prev_high  = prev_candle["high"]
    prev_low   = prev_candle["low"]

    prev_close_2 = prev_candle_2["close"]
    prev_open_2  = prev_candle_2["open"]
    prev_high_2  = prev_candle_2["high"]
    prev_low_2   = prev_candle_2["low"]

    return (open >= prev_close - 5.0 and prev_close > prev_open and
            open > close and valid_body and close < prev_low - 0.5 and
            prev_close_2 > prev_open_2 and
            open - close > prev_close - prev_open and prev_close > prev_close_2)

def is_bullish_engulfing(index, data):
    candle      = data.iloc[index]
    prev_candle = data.iloc[index-1]
    prev_candle_2 = data.iloc[index-2]

    close = candle["close"]
    open  = candle["open"]
    high  = candle["high"]
    low   = candle["low"]

    valid_body = close - open >= 7
    
    prev_close = prev_candle["close"]
    prev_open  = prev_candle["open"]
    prev_high  = prev_candle["high"]
    prev_low   = prev_candle["low"]

    prev_close_2 = prev_candle_2["close"]
    prev_open_2  = prev_candle_2["open"]
    prev_high_2  = prev_candle_2["high"]
    prev_low_2   = prev_candle_2["low"]

    return (open < prev_close + 5.0 and prev_open > prev_close and
            close > open and valid_body and close > prev_high + 0.5 and
            prev_open_2 > prev_close_2 and
            close - open > prev_open - prev_close and prev_close < prev_close_2)


def is_bearish_three_inside_down(index, data):
    candle      = data.iloc[index]
    prev_candle = data.iloc[index-1]
    prev_candle_2 = data.iloc[index-2]
    prev_candle_3 = data.iloc[index-3]

    close = candle["close"]
    open  = candle["open"]
    high  = candle["high"]
    low   = candle["low"]

    valid_body = open - close >= 7
    
    prev_close = prev_candle["close"]
    prev_open  = prev_candle["open"]
    prev_high  = prev_candle["high"]
    prev_low   = prev_candle["low"]

    prev_close_2 = prev_candle_2["close"]
    prev_open_2  = prev_candle_2["open"]
    prev_high_2  = prev_candle_2["high"]
    prev_low_2   = prev_candle_2["low"]

    prev_close_3 = prev_candle_3["close"]
    prev_open_3  = prev_candle_3["open"]
    prev_high_3  = prev_candle_3["high"]
    prev_low_3   = prev_candle_3["low"]

    return (prev_open >= prev_close_2 - 5.0 and prev_open > prev_close and valid_body and
            open > close and prev_close >= prev_low_2 + 0.5 and close < prev_low_2 and
            prev_close_2 > prev_open_2 and prev_close_2 > max(prev_close_3, prev_open_3))

def is_bullish_three_inside_up(index, data):
    candle      = data.iloc[index]
    prev_candle = data.iloc[index-1]
    prev_candle_2 = data.iloc[index-2]
    prev_candle_3 = data.iloc[index-3]

    close = candle["close"]
    open  = candle["open"]
    high  = candle["high"]
    low   = candle["low"]

    valid_body = close - open >= 7
    
    prev_close = prev_candle["close"]
    prev_open  = prev_candle["open"]
    prev_high  = prev_candle["high"]
    prev_low   = prev_candle["low"]

    prev_close_2 = prev_candle_2["close"]
    prev_open_2  = prev_candle_2["open"]
    prev_high_2  = prev_candle_2["high"]
    prev_low_2   = prev_candle_2["low"]

    prev_close_3 = prev_candle_3["close"]
    prev_open_3  = prev_candle_3["open"]
    prev_high_3  = prev_candle_3["high"]
    prev_low_3   = prev_candle_3["low"]

    return (prev_open < prev_close_2 + 5 and prev_close > prev_open and valid_body and
            close > open and prev_close <= prev_high_2 + 0.5 and close > prev_high_2 and
            prev_open_2 > prev_close_2 and prev_close_2 < min(prev_close_3, prev_open_3))
        
def is_bullish_marubozu(index, data):
    candle        = data.iloc[index]
    prev_candle   = data.iloc[index-1]
    prev_candle_2 = data.iloc[index-2]

    close = candle["close"]
    open  = candle["open"]
    high  = candle["high"]
    low   = candle["low"]

    prev_close = prev_candle["close"]
    prev_open  = prev_candle["open"]
    prev_high  = prev_candle["high"]
    prev_low   = prev_candle["low"]

    prev_close_2 = prev_candle_2["close"]
    prev_open_2  = prev_candle_2["open"]
    prev_high_2  = prev_candle_2["high"]
    prev_low_2   = prev_candle_2["low"]

    candle_body = abs(close - open)
    upper_wick  = high - max(close, open)
    lower_wick = min(close, open) - low
    max_allowed_wick_up = candle_body * 0.02
    max_allowed_wick_down = candle_body * 0.5

    cur_candle_body    = candle_body < 25 and candle_body > 10
    pre_candle_body   = abs(prev_close - prev_open) < 5 and abs(prev_high - prev_low) < 20
    pre_candle_body_2 = abs(prev_close_2 - prev_open_2) < 5 and abs(prev_high_2 - prev_low_2) < 20
    # pre_candle_body_3 = abs(prev_close_3 - prev_open_3) < 5 and abs(prev_high_3 - prev_low_3) < 20
    
    return (lower_wick <= max_allowed_wick_down and upper_wick <= max_allowed_wick_up and
            close > open and cur_candle_body and pre_candle_body and 
            pre_candle_body_2)


def is_bearish_marubozu(index, data):
    candle        = data.iloc[index]
    prev_candle   = data.iloc[index-1]
    prev_candle_2 = data.iloc[index-2]

    close = candle["close"]
    open  = candle["open"]
    high  = candle["high"]
    low   = candle["low"]

    prev_close = prev_candle["close"]
    prev_open  = prev_candle["open"]
    prev_high  = prev_candle["high"]
    prev_low   = prev_candle["low"]

    prev_close_2 = prev_candle_2["close"]
    prev_open_2  = prev_candle_2["open"]
    prev_high_2  = prev_candle_2["high"]
    prev_low_2   = prev_candle_2["low"]

    candle_body = abs(close - open)
    upper_wick  = high - max(close, open)
    lower_wick  = min(close, open) - low
    max_allowed_wick_up = candle_body * 0.5
    max_allowed_wick_down = candle_body * 0.02

    cur_candle_body   = candle_body < 25 and candle_body > 10
    pre_candle_body   = abs(prev_close - prev_open) < 5 and abs(prev_high - prev_low) < 20
    pre_candle_body_2 = abs(prev_close_2 - prev_open_2) < 5 and abs(prev_high_2 - prev_low_2) < 20
    # pre_candle_body_3 = abs(prev_close_3 - prev_open_3) < 5 and abs(prev_high_3 - prev_low_3) < 20

    return (lower_wick <= max_allowed_wick_down and upper_wick <= max_allowed_wick_up and
            close < open and cur_candle_body and pre_candle_body and pre_candle_body_2)

def bullish_candle(index, data):
    candle = data.iloc[index]
    
    close = candle["close"]
    open  = candle["open"]
    high  = candle["high"]
    low   = candle["low"]

    candle_length = high - low > 20.0
    body = close - open

    return (close > open and candle_length and body > 10.0 and
            (high - close <= body or open - low > body))

def bearish_candle(index, data):
    candle = data.iloc[index]
    
    close = candle["close"]
    open  = candle["open"]
    high  = candle["high"]
    low   = candle["low"]

    candle_length = high - low > 20.0
    body = open - close

    return (close < open and candle_length and body > 10.0 and
            (close - low <= body or high - close > body))

def is_candle_valid_for_buy(index, df):
    candle = df.iloc[index]
    
    close = candle["close"]
    open  = candle["open"]
    high  = candle["high"]
    low   = candle["low"]

    candle_length = high - low > 20.0
    body = close - open

    return (close > open and candle_length and body > 5.0)

def is_candle_valid_for_sell(index, df):
    candle = df.iloc[index]
    
    close = candle["close"]
    open  = candle["open"]
    high  = candle["high"]
    low   = candle["low"]

    candle_length = high - low > 20.0
    body = open - close

    return (close < open and candle_length and body > 5.0)
    

def valid_bulish_patterns(index, df):
    return is_hammer(index, df) or is_bullish_engulfing(index, df) or is_bullish_marubozu(index, df) or is_bullish_three_inside_up(index, df)

def valid_bearish_patterns(index, df):
    return is_shooting_star(index, df) or is_bearish_engulfing(index, df) or is_bearish_marubozu(index, df) or is_bearish_three_inside_down(index, df)

def is_bullish_candle(index, df):
    return single_hammer(index, df) or bullish_candle(index, df)

def is_bearish_candle(index, df):
    return single_shooting_star(index, df) or bearish_candle(index, df)
    
    