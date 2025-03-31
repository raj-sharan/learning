#!/Users/grajwade/vPython/bin/python

import re
import sys
import json
import time
import logging
import traceback
from datetime import datetime

import pandas as pd

from settings import Setting
from instrument import Instrument
from  moving_averages import *
from  candlestick_patterns import *
from common import Util


logging.basicConfig(stream=sys.stdout, level=logging.DEBUG,
	format="%(asctime)s[%(levelname)s] - %(message)s")

token = 256265
is_index = True

setting = Setting()

instrument = Instrument(token, is_index, setting, logging)
instrument.load_historical_test_data()


instrument.historical_data_5m["SMA-200"] = moving_average_close_200sma(instrument.historical_data_5m)
instrument.historical_data_5m["SMA-20"] = moving_average_close_20sma(instrument.historical_data_5m)
instrument.historical_data_5m["ATR"] = atr(instrument.historical_data_5m)
instrument.historical_data_5m.drop(columns=['previous_close', 'high_low', 'high_close', 'low_close'], inplace=True)

# instrument.historical_data_30m["SMA-9-h"] = moving_average_high_9sma(instrument.historical_data_30m)
# instrument.historical_data_30m["SMA-9-l"] = moving_average_low_9sma(instrument.historical_data_30m)

# print(instrument.historical_data_5m)
# print(instrument.historical_data_30m)

df_5m = instrument.historical_data_5m
df_30m = instrument.historical_data_30m

df_5m["shooting_star"] = df_5m.apply(lambda row: is_shooting_star(row.name, df_5m), axis=1)
# df_5m["is_bearish_engulfing"] = df_5m.apply(lambda row: is_bearish_engulfing(row.name, df_5m), axis=1)
# df_5m["is_bearish_marubozu"] = df_5m.apply(lambda row: is_bearish_marubozu(row.name, df_5m), axis=1)
df_5m["is_hammer"] = df_5m.apply(lambda row: is_hammer(row.name, df_5m), axis=1)
# df_5m["is_bullish_engulfing"] = df_5m.apply(lambda row: is_bullish_engulfing(row.name, df_5m), axis=1)
# df_5m["is_bullish_marubozu"] = df_5m.apply(lambda row: is_bullish_marubozu(row.name, df_5m), axis=1)

# df_pe = df_5m[df_5m[["shooting_star", "is_bearish_engulfing", "is_bearish_marubozu"]].any(axis=1)]
# df_pe.shape

# df_ce = df_5m[df_5m[["is_hammer", "is_bullish_engulfing", "is_bullish_marubozu"]].any(axis=1)]
# df_ce.shape

# df_patterns = df_5m[df_5m[["is_hammer", "is_bullish_engulfing", "is_bullish_marubozu", "shooting_star", "is_bearish_engulfing", "is_bearish_marubozu"]].any(axis=1)]

result_df = pd.DataFrame()
profit = 0
for index, row in df_5m.iterrows():
    if index < 200 or index > len(df_5m) - 6:
        continue

    if row["is_hammer"]:
    # if row["is_hammer"] or row["is_bullish_engulfing"]:
        candle_5m = row.copy()
        print(index, candle_5m["SMA-20"] > candle_5m["SMA-200"] + 2, candle_5m["low"] <= candle_5m["SMA-20"] + 7 , 11)
        if True or candle_5m["SMA-20"] > candle_5m["SMA-200"] + 2 and candle_5m["low"] <= candle_5m["SMA-20"] + 7:
            pre_candle_5m = df_5m.iloc[index - 1]
            print(index, candle_5m["SMA-20"] > pre_candle_5m["SMA-20"] + 0.1, 12)
            if True or candle_5m["SMA-20"] > pre_candle_5m["SMA-20"] + 0.1:
                target = round(row['high'] + row['ATR'], 2)
                stop_loss = row['low'] - 5
                pnl = 0
                points = 0

                if abs(row['high'] - stop_loss) < 30 and target - row['high'] > 20:
                    # Next 6 candles check target and stop loss
                    for i in range(1, 6):
                        if index + i < len(df_5m):  # Ensure index is within bounds
                            next_row = df_5m.iloc[index + i]
                            if next_row['low'] <= stop_loss:
                                pnl = stop_loss - row['high']
                                break
                            elif next_row['high'] >= target:
                                pnl = target - row['high']
                                break
                            else:
                                points = row['close'] - row['high']
        
                    pnl = pnl if pnl != 0 else points
                    profit = profit + pnl
                    candle_5m['PnL'] = round(pnl, 2)
                    result_df = pd.concat([result_df, pd.DataFrame([candle_5m])], ignore_index=True)


    if row["shooting_star"]:
    # if row["shooting_star"] or row["is_bearish_engulfing"]:
        candle_5m = row.copy()
        print(index, candle_5m["SMA-20"] < candle_5m["SMA-200"] - 2, candle_5m["high"] >= candle_5m["SMA-20"] - 7, 21)
        if True or candle_5m["SMA-20"] < candle_5m["SMA-200"] - 2 and candle_5m["high"] >= candle_5m["SMA-20"] - 7:
            pre_candle_5m = df_5m.iloc[index - 1]
            print(index, candle_5m["SMA-20"] < pre_candle_5m["SMA-20"], 22)
            if True or candle_5m["SMA-20"] < pre_candle_5m["SMA-20"] - 0.1:
                target = round(row['low'] - row['ATR'], 2)
                stop_loss = row['high'] + 3
                pnl = 0
                points = 0
                if abs(stop_loss - row['low']) < 30 and row['low'] - target > 20:
                    # Next 6 candles check target and stop loss
                    for i in range(1, 6):
                        if index + i < len(df_5m):  # Ensure index is within bounds
                            next_row = df_5m.iloc[index + i]
                            if next_row['high'] >= stop_loss:
                                pnl = row['low'] - stop_loss
                                break
                            elif next_row['low'] <= target:
                                pnl = row['low'] - target
                                break
                            else:
                                points = row['low'] - row['close']
    
                    pnl = pnl if pnl != 0 else points
                    profit = profit + pnl
                    candle_5m['PnL'] = round(pnl, 2)
                    result_df = pd.concat([result_df, pd.DataFrame([candle_5m])], ignore_index=True)

print(f"Points : {profit}")
result_df.to_csv("NIFTY50-poc.csv", index=False)

print('Done')



