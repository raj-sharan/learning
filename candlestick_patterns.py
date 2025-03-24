from datetime import datetime

def is_shooting_star(index, data):
    candle      = data.iloc[index]
    
    close = candle["close"]
    open  = candle["open"]
    high  = candle["high"]
    low   = candle["low"]

    candle_length = high - low > 10

    return (close < open and candle_length and
            high - max(open, close) >= abs(open - close) * 1.5 and
            min(close, open) - low <= abs(open - close))


def is_hammer(index, data):
    candle = data.iloc[index]
    prev_candle = data.iloc[index-1]
    
    close = candle["close"]
    open  = candle["open"]
    high  = candle["high"]
    low   = candle["low"]

    candle_length = high - low > 10

    return (close > open and candle_length and
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
 
    return (open >= prev_close - 5 and prev_close > prev_open and
            open > close and valid_body and
            prev_open > close and prev_close_2 > prev_open_2 and
            open - close > prev_close - prev_open and prev_open > prev_open_2)
        

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

    return (open < prev_close + 5 and prev_open > prev_close and
            close > open and valid_body and
            close > prev_open and prev_open_2 > prev_close_2 and
            close - open > prev_open - prev_close and prev_open < prev_open_2)


def is_bearish_harami(index, data):
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

    return (prev_close > prev_open and
            prev_open <= close < open <= prev_close and
            open - close < prev_close - prev_open)

    # return (prev_close > prev_open and
    #        abs(prev_close - prev_open) / (prev_high - prev_low) >= 0.7 and
    #        0.3 > abs(close - open) / (high - low) >= 0.1 and
    #        high < prev_close and
    #        low > prev_open)

def is_bullish_harami(index, data):
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

    return (prev_open > prev_close and
            prev_close <= open < close <= prev_open and
            close - open < prev_open - prev_close)

    # return (prev_close < prev_open and
    #        abs(prev_close - prev_open) / (prev_high - prev_low) >= 0.7
    #        and 0.3 > abs(close - open) / (high - low) >= 0.1
    #        and high < prev_open
    #        and low > prev_close)
        
def is_bullish_marubozu(index, data):
    candle        = data.iloc[index]
    prev_candle   = data.iloc[index-1]
    prev_candle_2 = data.iloc[index-2]
    # prev_candle_3 = data.iloc[index-3]

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

    # prev_close_3 = prev_candle_3["close"]
    # prev_open_3  = prev_candle_3["open"]
    # prev_high_3  = prev_candle_3["high"]
    # prev_low_3   = prev_candle_3["low"]

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
    # prev_candle_3 = data.iloc[index-2]

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

    # prev_close_3 = prev_candle_3["close"]
    # prev_open_3  = prev_candle_3["open"]
    # prev_high_3  = prev_candle_3["high"]
    # prev_low_3   = prev_candle_3["low"]

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


def valid_bulish_patterns(index, df):
    return is_hammer(index, df) or is_bullish_engulfing(index, df) or is_bullish_marubozu(index, df)

def valid_bearish_patterns(index, df):
    return is_shooting_star(index, df) or is_bearish_engulfing(index, df) or is_bearish_marubozu(index, df)