import pandas as pd
from datetime import datetime, timedelta

from momentum_analyser import MomentumAnalyser

class LiveData:
    def __init__(self, setting, logging):  # Fixed constructor
        self.logging = logging
        self.instruments_data = {}  # Initialized as an empty dictionary
        self.order_updated = False
        self.analyser = MomentumAnalyser(setting, logging)
    
    def collect_instruments_data(self, ticks):
        self.analyser.load_ticks(ticks)
        
        for tick in ticks:
            token = tick["instrument_token"]
            price = tick["last_price"]
            volume_traded = tick['volume_traded'] if 'volume_traded' in tick else 0
            bid_volume, offer_volume = self.analyser.fetch_bid_offer_volume(tick)
            oi = tick['oi'] if 'oi' in tick else 0
            
            if token not in self.instruments_data:
                self.instruments_data[token] = {}
            
            self.instruments_data[token]['price'] = price
            self.instruments_data[token]['time'] = datetime.now()  # Update time on new data
            self.instruments_data[token]['volume_traded'] = volume_traded
            self.instruments_data[token]['oi'] = oi
            self.instruments_data[token]['bid_volume'] = bid_volume
            self.instruments_data[token]['offer_volume'] = offer_volume

    def to_s(self):
        flag = 1
        # df = pd.DataFrame.from_dict(self.instruments_data, orient="index")
        # df.reset_index(drop=True, inplace=True)
        # if len(df) > 0:
        #     print('---------------------------------------------------------')
        #     print(df)
        #     print('---------------------------------------------------------')
        
    def get_current_data(self, token):  # Added `self`
        if self.instruments_data is not None and token in self.instruments_data:
            if (datetime.now() - self.instruments_data[token]['time']) <= timedelta(minutes=2): 
                return self.instruments_data[token]
        return None

        
    # def get_current_pattern(token, time_id):
    #     data_5m["is_shooting_star"]     = data_5m.apply(lambda row: is_shooting_star(row.name, data_5m), axis=1)
    #     data_5m["is_hammer"]            = data_5m.apply(lambda row: is_hammer(row.name, data_5m), axis=1)
    #     data_5m["is_bearish_engulfing"] = data_5m.apply(lambda row: is_bearish_engulfing(row.name, data_5m), axis=1)
    #     data_5m["is_bullish_engulfing"] = data_5m.apply(lambda row: is_bullish_engulfing(row.name, data_5m), axis=1)
    #     data_5m["is_bearish_harami"]    = data_5m.apply(lambda row: is_bearish_harami(row.name, data_5m), axis=1)
    #     data_5m["is_bullish_harami"]    = data_5m.apply(lambda row: is_bullish_harami(row.name, data_5m), axis=1)
    #     data_5m["is_bullish_marubozu"]  = data_5m.apply(lambda row: is_bullish_marubozu(row.name, data_5m), axis=1)
    #     data_5m["is_bearish_marubozu"]  = data_5m.apply(lambda row: is_bearish_marubozu(row.name, data_5m), axis=1)
        
    #     data_5m[time_id]
		
        