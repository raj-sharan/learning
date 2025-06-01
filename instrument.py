from datetime import datetime, timedelta
import pandas as pd
import traceback

from moving_averages import *
from candlestick_patterns import *
from common import Util
from order import MarketOrder
from db_connect import PostgresDB
from historetical_data import HistoricalData

class Instrument:

    def __init__(self, token, is_index, setting, logging):
        self.token = token
        self.is_index = is_index
        self.setting = setting
        self.logging = logging
        self.refresh_till_5m = None
        self.refresh_till_30m = None
        self.orders = {} 
        self.historical_data_5m = None
        self.historical_data_30m = None
        self.low_margin_at = None
        self.db_conn = PostgresDB(setting, logging)
        self.momentum_result = {}
        self.current_data_analysis = {}
        self.premium_data = {}
        self.last_order_key = None
        self.market_trend = {'low': None, 'high': None, 'direction': None, 'change': False}


    def order_ids(self):
        return self.orders.keys()
        
    def load_historical_data(self, unique_key):
        historical_data = HistoricalData(self.setting, self.token, self.logging)
        self.historical_data_5m  = historical_data.load_5min_data(unique_key)
        self.historical_data_30m = historical_data.load_30min_data()

    def refresh_data(self, kite_login, current_time):
        end_dt = datetime(current_time.year, current_time.month, current_time.day, 15, 30)
        if current_time > end_dt:
            current_time = end_dt
           
        if not self.historical_data_5m.empty:
            from_dt = datetime(current_time.year, current_time.month, current_time.day, 9, 15)
            if self.refresh_till_5m is None: 
                self.refresh_till_5m = from_dt

            if current_time > self.refresh_till_5m and current_time - self.refresh_till_5m > timedelta(minutes = 5):
                minutes_since_from_dt = (current_time - from_dt).total_seconds() // 60
                candle_count = int(minutes_since_from_dt // 5)
            
                to_dt = from_dt + timedelta(minutes = candle_count * 5)
                if from_dt < to_dt:
                    try:
                        historical_data = HistoricalData(self.setting, self.token, self.logging)
                        data_df = historical_data.load_5m_current_data(from_dt, to_dt)
                        if data_df.empty:
                            data = kite_login.conn.historical_data(self.token, from_dt, to_dt, "5minute")
                            data_df = pd.DataFrame(data)
     
                        if len(data_df) == candle_count:
                            data_df = data_df.drop(columns=["volume"]) if 'volume' in data_df.columns else data_df

                            data_df['date'] = data_df.apply(lambda row: Util.parse_datetime(row["date"]), axis=1)
                            data_df["unique_key"] = data_df.apply(lambda row: Util.generate_5m_id(row["date"]), axis=1)
                            data_df["token"]      = data_df.apply(lambda row: self.token, axis = 1)
                            threshold = Util.generate_id(from_dt)
                            self.historical_data_5m.drop(self.historical_data_5m[self.historical_data_5m['unique_key'] >= threshold].index, inplace=True)
                            self.historical_data_5m = pd.concat([self.historical_data_5m, data_df], ignore_index=True)
                            self.historical_data_5m["SMA-200"] = moving_average_close_200sma(self.historical_data_5m)
                            self.historical_data_5m["SMA-20"] = moving_average_close_20sma(self.historical_data_5m)
                            self.historical_data_5m["SMA-9"] = moving_average_close_9sma(self.historical_data_5m)
                            self.refresh_till_5m = to_dt
                            self.logging.info(f"Refreshed 5m data for {self.token} till {self.refresh_till_5m}")
                    except Exception as e:
                        self.logging.error(f"Error in refresh 5m data for {self.token}: {e}")
        
        return

    def load_momentum_analysis(self, kite_login, live_data, instrument_token, current_time):
        if self.historical_data_5m.empty or "SMA-20" not in self.historical_data_5m:
            return 
            
        if self.refresh_till_5m and current_time - self.refresh_till_5m <= timedelta(minutes = 5):
            candle_5m = self.historical_data_5m.iloc[-1]
            unique_key = Util.generate_5m_id(candle_5m['date'])
            
            if not self.momentum_result or self.momentum_result['unique_key'] != unique_key:
                self.load_trend_direction()

                candle_set = self.historical_data_5m.tail(4)
               
                is_bullish = valid_bulish_patterns(len(candle_set) - 1, candle_set)
                is_bearish = valid_bearish_patterns(len(candle_set) - 1, candle_set)
                bullish_candle = is_bullish_candle(len(candle_set) - 1, candle_set)
                bearish_candle = is_bearish_candle(len(candle_set) - 1, candle_set)
                buy_candle = is_candle_valid_for_buy(len(candle_set) - 1, candle_set)
                sell_candle = is_candle_valid_for_sell(len(candle_set) - 1, candle_set)
                    
                self.momentum_result = {
                    'parent_token' : self.token,
                    'unique_key': unique_key,
                    'date': candle_5m['date'],
                    'is_bullish': is_bullish,
                    'is_bearish': is_bearish,
                    'is_bullish_candle': bullish_candle,
                    'is_bearish_candle': bearish_candle,
                    'buy_candle': buy_candle,
                    'sell_candle': sell_candle
                }
                
    def load_current_data_analysis(self, live_data, instrument_token, current_date):
        analyser = live_data.analyser
        momentum = analyser.analyse_oi_momentum(instrument_token, live_data, self.order_ids())

        if momentum is None:
            return None

        token_data = live_data.get_current_data(self.token)
        strike_price_tokens = self.get_ce_pe_tokens(instrument_token, token_data)
        if strike_price_tokens is None:
            return None

        ce_token = strike_price_tokens['curr_ce_token']
        pe_token = strike_price_tokens['curr_pe_token']
        ce_token_data = live_data.get_current_data(ce_token)
        pe_token_data = live_data.get_current_data(pe_token)

        if ce_token and pe_token and ce_token_data and pe_token_data:
            oi_momentum = analyser.calculate_oi_change(current_date, strike_price_tokens)
            
            token_price = token_data['price']
        
            self.current_data_analysis['date'] = current_date
            self.current_data_analysis['unique_key'] = Util.generate_5m_id(current_date)

            self.current_data_analysis['ce_token'] = ce_token
            self.current_data_analysis['pe_token'] = pe_token
            self.current_data_analysis['ce_symbol'] = instrument_token.get_symbols_from_tokens([ce_token])[0]
            self.current_data_analysis['pe_symbol'] = instrument_token.get_symbols_from_tokens([pe_token])[0]
        
            self.current_data_analysis['last_price'] = token_price
            self.current_data_analysis['ce_price'] = ce_token_data['price']
            self.current_data_analysis['pe_price'] = pe_token_data['price']

            self.current_data_analysis['pcr_nearest'] = momentum['pcr_nearest']
            self.current_data_analysis['pcr_next'] = momentum['pcr_next']
            self.current_data_analysis['pcr_order'] = momentum['pcr_order']
            self.current_data_analysis['oi_nearest'] = momentum['oi_nearest']
            self.current_data_analysis['oi_next'] = momentum['oi_next']
            self.current_data_analysis['strike_range'] = momentum['strike_range'] 

            self.current_data_analysis['pcr_pre_nearest'] = momentum['pcr_pre_nearest']
            self.current_data_analysis['pcr_pre_next'] = momentum['pcr_pre_next']
            self.current_data_analysis['oi_pre_nearest'] = momentum['oi_pre_nearest']
            self.current_data_analysis['oi_pre_next'] = momentum['oi_pre_next']
            self.current_data_analysis['pre_strike_range'] = momentum['pre_strike_range']

            nearest_strike = self.current_data_analysis['strike_range'][0]
            next_strike = self.current_data_analysis['strike_range'][1]

            nearest_strike_tokens = strike_price_tokens[nearest_strike]
            next_strike_tokens = strike_price_tokens[next_strike]

            self.current_data_analysis['nearest_ce_oi_change'] = self.get_oi_change_volue(oi_momentum, nearest_strike_tokens['CE']['instrument_token'])
            self.current_data_analysis['nearest_pe_oi_change'] = self.get_oi_change_volue(oi_momentum, nearest_strike_tokens['PE']['instrument_token'])
            
            self.current_data_analysis['next_ce_oi_change'] = self.get_oi_change_volue(oi_momentum, next_strike_tokens['CE']['instrument_token'])
            self.current_data_analysis['next_pe_oi_change'] = self.get_oi_change_volue(oi_momentum, next_strike_tokens['PE']['instrument_token'])

            self.current_data_analysis['high_ce_oi_strike'] = high_ce_oi_strike = self.get_high_oi_strike(oi_momentum, 'high_ce_oi_strike')
            self.current_data_analysis['high_pe_oi_strike'] = high_pe_oi_strike = self.get_high_oi_strike(oi_momentum, 'high_pe_oi_strike')

            self.current_data_analysis['direction'] = self.market_trend['direction']
            self.current_data_analysis['nearest_gap'] = nearest_gap = token_price - nearest_strike
            self.current_data_analysis['next_gap'] = next_gap = next_strike - token_price
            self.current_data_analysis['signal'] = None
            
            pcr_next = momentum['pcr_next']
            pcr_nearest = momentum['pcr_nearest']
            direction = self.market_trend['direction']

            is_bullish = bullish_candle = buy_candle = False
            is_bearish = bearish_candle = sell_candle = False
            
            if self.momentum_result:
                is_bullish =  self.momentum_result['is_bullish']
                is_bearish =  self.momentum_result['is_bearish']
                bullish_candle = self.momentum_result['is_bullish_candle']
                bearish_candle = self.momentum_result['is_bearish_candle']
                buy_candle = self.momentum_result['buy_candle']
                sell_candle = self.momentum_result['sell_candle']

            down_direction = self.current_data_analysis['next_ce_oi_change']['oi_trend'] == 'Increasing'
            down_oi_change = self.current_data_analysis['next_ce_oi_change']['oi_change'] > 0.4

            up_direction = self.current_data_analysis['nearest_pe_oi_change']['oi_trend'] == 'Increasing'
            up_oi_change = self.current_data_analysis['nearest_pe_oi_change']['oi_change'] > 0.4

            if not self.setting.selling and pcr_nearest != 0 and pcr_next != 0:
                if pcr_nearest > 1.5 and self.current_data_analysis['oi_nearest'][0] > 7500000 and next_gap > 30:
                    if self.current_data_analysis['oi_next'][1] < 7500000 or (self.current_data_analysis['oi_next'][1] > 7500000 and pcr_next > 1.5):
                        if (direction == 'Up' or bullish_candle or is_bullish) and buy_candle and up_direction and up_oi_change:
                            self.current_data_analysis['signal'] = 'Buy CE'
                elif pcr_next < 0.75 and self.current_data_analysis['oi_next'][1] > 7500000 and nearest_gap > 30:
                    if self.current_data_analysis['oi_nearest'][0] < 7500000 or (self.current_data_analysis['oi_nearest'][0] > 7500000 and pcr_nearest < 0.95):
                        if (direction == 'Down' or bearish_candle or is_bearish) and sell_candle and down_direction and down_oi_change:
                            self.current_data_analysis['signal'] = 'Buy PE'
                        
            if self.setting.selling and pcr_nearest != 0 and pcr_next != 0:
                if pcr_nearest > 2.0 and self.current_data_analysis['oi_nearest'][0] > 7500000 and pcr_next > 0.7 and self.current_data_analysis['oi_next'][1] < 7500000:
                    if (direction == 'Up' or bullish_candle or is_bullish) and buy_candle:
                        self.current_data_analysis['signal'] = 'Sell PE'
                elif pcr_next < 0.5 and self.current_data_analysis['oi_next'][1] > 7500000 and pcr_nearest < 1.3 and self.current_data_analysis['oi_nearest'][0] < 7500000:
                    if (direction == 'Down' or bearish_candle or is_bearish) and sell_candle:
                        self.current_data_analysis['signal'] = 'Sell CE'

    def get_oi_change_volue(self, oi_momentum, token):
        token_result = {
            "oi_change": 0.0,
            "oi_trend": ""
        }
        if token not in oi_momentum:
            return token_result

        return oi_momentum[token]

    def get_high_oi_strike(self, oi_momentum, key):
        if key not in oi_momentum:
            return -1

        return oi_momentum[key]
        
    
    def set_trend_to_symbol(self, trend):
        if trend == "Increasing":
            return '↑'
        elif trend == 'Decreasing':
            return '↓'
        else:
            return '-'
            
                
    def print_analysis_details(self, should_save = True):
        # Create DataFrame with the desired columns
        print_df = pd.DataFrame(columns=['', '|', '', '|', '', '|', '','',''])
        print_df.loc[len(print_df)] = ["--Strike Price--", '|', "----PE OI-----", '|', "----CE OI----", '|', "----PCR---",'','']
        if self.current_data_analysis:
            next_pre_strike = self.current_data_analysis['pre_strike_range'][1]
            next_pre_pe_oi = self.current_data_analysis['oi_pre_next'][0]
            next_pre_ce_oi = self.current_data_analysis['oi_pre_next'][1]
            next_pre_pcr = f"{self.current_data_analysis['pcr_pre_next']:.4f}".rstrip('0').rstrip('.')

            next_pre_pe_oi_str = f'{self.format_oi(next_pre_pe_oi)}'
            next_pre_ce_oi_str = f'{self.format_oi(next_pre_ce_oi)}'

            print_df.loc[len(print_df)] = [next_pre_strike, '|', next_pre_pe_oi_str, '|', next_pre_ce_oi_str, '|', next_pre_pcr,'','']
            
            next_strike = self.current_data_analysis['strike_range'][1]
            next_pe_oi = self.current_data_analysis['oi_next'][0]
            next_ce_oi = self.current_data_analysis['oi_next'][1]
            next_pcr = f"{self.current_data_analysis['pcr_next']:.4f}".rstrip('0').rstrip('.')

            sign = '+' if self.current_data_analysis['next_ce_oi_change']['oi_change'] > 0 else '-'
            next_ce_oi_change_pct = f"{sign}{abs(self.current_data_analysis['next_ce_oi_change']['oi_change']):.2f}".rstrip('.')
            next_ce_oi_change_trend = self.current_data_analysis['next_ce_oi_change']['oi_trend']
            sign = '+' if self.current_data_analysis['next_pe_oi_change']['oi_change'] > 0 else '-'
            next_pe_oi_change_pct = f"{sign}{abs(self.current_data_analysis['next_pe_oi_change']['oi_change']):.2f}".rstrip('.')
            next_pe_oi_change_trend = self.current_data_analysis['next_pe_oi_change']['oi_trend']

            next_pe_oi_str = f'{self.format_oi(next_pe_oi)} ({next_pe_oi_change_pct}) {self.set_trend_to_symbol(next_pe_oi_change_trend)}'
            next_ce_oi_str = f'{self.format_oi(next_ce_oi)} ({next_ce_oi_change_pct}) {self.set_trend_to_symbol(next_ce_oi_change_trend)}'

            print_df.loc[len(print_df)] = [next_strike, '|', next_pe_oi_str, '|', next_ce_oi_str, '|', next_pcr,'','']
            
            nearest_strike = self.current_data_analysis['strike_range'][0]
            nearest_pe_oi = self.current_data_analysis['oi_nearest'][0]
            nearest_ce_oi = self.current_data_analysis['oi_nearest'][1]
            nearest_pcr = f"{self.current_data_analysis['pcr_nearest']:.4f}".rstrip('0').rstrip('.')

            sign = '+' if self.current_data_analysis['nearest_ce_oi_change']['oi_change'] > 0 else '-'
            nearest_ce_oi_change_pct = f"{sign}{abs(self.current_data_analysis['nearest_ce_oi_change']['oi_change']):.2f}".rstrip('.')
            nearest_ce_oi_change_trend = self.current_data_analysis['nearest_ce_oi_change']['oi_trend']
            sign = '+' if self.current_data_analysis['nearest_pe_oi_change']['oi_change'] > 0 else '-'
            nearest_pe_oi_change_pct = f"{sign}{abs(self.current_data_analysis['nearest_pe_oi_change']['oi_change']):.2f}".rstrip('.')
            nearest_pe_oi_change_trend = self.current_data_analysis['nearest_pe_oi_change']['oi_trend']

            nearest_pe_oi_str = f'{self.format_oi(nearest_pe_oi)} ({nearest_pe_oi_change_pct}) {self.set_trend_to_symbol(nearest_pe_oi_change_trend)}'
            nearest_ce_oi_str = f'{self.format_oi(nearest_ce_oi)} ({nearest_ce_oi_change_pct}) {self.set_trend_to_symbol(nearest_ce_oi_change_trend)}'

            print_df.loc[len(print_df)] = [nearest_strike, '|', nearest_pe_oi_str, '|', nearest_ce_oi_str, '|', nearest_pcr,'','']

            nearest_pre_strike = self.current_data_analysis['pre_strike_range'][0]
            nearest_pre_pe_oi = self.current_data_analysis['oi_pre_nearest'][0]
            nearest_pre_ce_oi = self.current_data_analysis['oi_pre_nearest'][1]
            nearest_pre_pcr = f"{self.current_data_analysis['pcr_pre_nearest']:.4f}".rstrip('0').rstrip('.')

            nearest_pe_oi_str = f'{self.format_oi(nearest_pre_pe_oi)}'
            nearest_ce_oi_str = f'{self.format_oi(nearest_pre_ce_oi)}'

            print_df.loc[len(print_df)] = [nearest_pre_strike, '|', nearest_pe_oi_str, '|', nearest_ce_oi_str, '|', nearest_pre_pcr,'','']

        print_df.loc[len(print_df)] = ["---------------", '|', "---------------", '|', "---------------", '|', "--------------", '', '']

        trend = None
        candle = None
        # Add rows from momentum_result
        if self.momentum_result:
            trend = 'Bullish' if self.momentum_result['is_bullish'] else None
            trend = 'Bearish' if trend is None and self.momentum_result['is_bearish'] else None
            candle = 'Bullish Candle' if self.momentum_result['is_bullish_candle'] else None
            candle = 'Bearish Candle' if candle is None and self.momentum_result['is_bearish_candle'] else None
            
        trend = trend if trend else ''
        candle = candle if candle else ''
            
        if self.current_data_analysis:
            ce_symbol = self.current_data_analysis['ce_symbol']
            pe_symbol = self.current_data_analysis['pe_symbol']
            ce_price = self.current_data_analysis['ce_price']
            pe_price = self.current_data_analysis['pe_price']
            
            nearest_gap = f"{self.current_data_analysis['nearest_gap']:.4f}".rstrip('0').rstrip('.')
            next_gap = f"{self.current_data_analysis['next_gap']:.4f}".rstrip('0').rstrip('.')
            direction = self.current_data_analysis['direction'] if self.current_data_analysis['direction'] else ''
            
            print_df.loc[len(print_df)] = ["Candle Pattern", '|', trend or '-', '|', direction or '-','|', candle or '-','','']
            print_df.loc[len(print_df)] = ["PE Token", '|', pe_symbol, '|', pe_price,'|', f'+{next_gap}','', '']
            print_df.loc[len(print_df)] = ["CE Token", '|', ce_symbol, '|', ce_price,'|', f'-{nearest_gap}','', '']
    

        print_df.loc[len(print_df)] = ["---------------", '|', "---------------", '|', "---------------", '|', "--------------", '', '']
        # Display the DataFrame nicely
        print(print_df)

        date = self.momentum_result['date'] if self.momentum_result else ''
        price = self.current_data_analysis['last_price'] if self.current_data_analysis else 0.0
        signal = self.current_data_analysis['signal'] if self.current_data_analysis else ''
        print(f'Signal : {signal or ''} Last candle at {date or '-'}, Last Price: {price or 0.0}')
    
        columns_order = [
            'unique_key', 'date', 'trend', 'direction', 'signal', 'last_price', 'candle',
            'nearest_strike', 'nearest_pe_oi', 'nearest_ce_oi', 'nearest_pcr', 'nearest_gap',
            'next_strike', 'next_pe_oi', 'next_ce_oi', 'next_pcr', 'next_gap'
        ]
        if self.current_data_analysis:
            data_to_print = pd.DataFrame(columns=columns_order)
            data_to_print = data_to_print._append({
                'unique_key': self.current_data_analysis['unique_key'],
                'date': self.current_data_analysis['date'],
                'trend': trend if trend else '',
                'direction': direction,
                'signal': signal if signal else '',
                'last_price': price,
                'candle': candle,
                'nearest_strike': nearest_strike,
                'nearest_pe_oi': nearest_pe_oi,
                'nearest_ce_oi': nearest_ce_oi,
                'nearest_pcr': nearest_pcr,
                'nearest_gap': nearest_gap,
                'next_strike': next_strike,
                'next_pe_oi': next_pe_oi,
                'next_ce_oi': next_ce_oi,
                'next_pcr': next_pcr,
                'next_gap': next_gap
            }, ignore_index=True)

            if should_save:
                self.save_data_to_db(data_to_print)
        
    def execute_trade_opportunity(self, kite_login, live_data, instrument_token, current_time):
        if self.refresh_till_5m is None or not self.momentum_result or not self.current_data_analysis:
            return

        last_order_old = self.last_order_key is None or self.last_order_key != self.momentum_result['unique_key']
        if last_order_old and current_time > self.refresh_till_5m and current_time - self.refresh_till_5m < timedelta(minutes = 5):
            candle_5m = self.historical_data_5m.iloc[-1]
            check_unique_key = (
                candle_5m['unique_key'] == self.momentum_result['unique_key']
            )
            if check_unique_key and self.current_data_analysis: 
                candle_5m = self.historical_data_5m.iloc[-1]
                prev_candle_5m = self.historical_data_5m.iloc[-2]

                pre_sma_20 = prev_candle_5m["SMA-20"]
                sma_20 = candle_5m["SMA-20"]
                sma_9 = candle_5m["SMA-9"]
                sma_200 = candle_5m["SMA-200"]
                is_bullish = self.momentum_result['is_bullish']
                is_bearish = self.momentum_result['is_bearish']
                nearest_pcr = self.current_data_analysis['pcr_nearest']
                next_pcr = self.current_data_analysis['pcr_next']

                is_baught = False
                # if is_bullish:
                #     on_line = (sma_20 + 5 >= candle_5m["low"] or sma_9 + 3 >= candle_5m["low"]) and sma_20 > pre_sma_20 + 1 and sma_20 > sma_200
                #     if on_line and (nearest_pcr > 1.0 or next_pcr > 1.0):
                #         is_baught = True
                #         print(f'✅ Buy CE (Bullish) at {candle_5m['date']}')
                #         self.buy_premium('CE', kite_login, live_data, instrument_token, current_time)   

                # elif is_bearish:
                #     on_line = (sma_20 - 5 <= candle_5m["high"] or sma_9 - 3 <= candle_5m["high"]) and sma_20 < pre_sma_20 - 1 and sma_20 < sma_200
                #     if on_line and (nearest_pcr < 0.7 or next_pcr < 0.7):
                #         is_baught = True
                #         print(f'❌ Buy PE (Bearish) at {candle_5m['date']}')
                #         self.buy_premium('PE', kite_login, live_data, instrument_token, current_time)
                            
                if not is_baught and self.current_data_analysis['signal']:
                    if self.current_data_analysis['signal'] == 'Buy CE':
                        print(f'✅ Buy CE at {current_time}')
                        self.buy_premium('CE', kite_login, live_data, instrument_token, current_time, True)
                        
                    elif self.current_data_analysis['signal'] == 'Buy PE':
                        print(f'❌ Buy PE at {current_time}')
                        self.buy_premium('PE', kite_login, live_data, instrument_token, current_time, True)

                    elif self.current_data_analysis['signal'] == 'Sell CE':
                        print(f'✅ Sell CE at {current_time}')
                        self.sell_premium('CE', kite_login, live_data, instrument_token, current_time, True)

                    elif self.current_data_analysis['signal'] == 'Sell PE':
                        print(f'❌ Sell PE at {current_time}')
                        self.sell_premium('PE', kite_login, live_data, instrument_token, current_time, True)
                                

    def buy_premium(self, ce_pe, kite_login, live_data, instrument_token, current_time, scalping = False):
        session_end_dt = datetime(current_time.year, current_time.month, current_time.day, 14, 55)
        if current_time > session_end_dt:
            return
        
        if ce_pe == 'CE':
            self.buy_ce_premium(kite_login, live_data, instrument_token, current_time, scalping)
        elif ce_pe == 'PE':
            self.buy_pe_premium(kite_login, live_data, instrument_token, current_time, scalping)

    def sell_premium(self, ce_pe, kite_login, live_data, instrument_token, current_time, scalping = False):
        session_end_dt = datetime(current_time.year, current_time.month, current_time.day, 14, 55)
        if current_time > session_end_dt:
            return
        
        if ce_pe == 'CE':
            self.sell_ce_premium(kite_login, live_data, instrument_token, current_time, scalping)
        elif ce_pe == 'PE':
            self.sell_pe_premium(kite_login, live_data, instrument_token, current_time, scalping)
            
    def buy_ce_premium(self, kite_login, live_data, instrument_token, current_time, scalping):
        try:   
            ce_token = self.current_data_analysis['ce_token']
  
            from_dt = datetime(current_time.year, current_time.month, current_time.day, 9, 15)

            minutes_since_from_dt = (current_time - from_dt).total_seconds() // 60
            candle_count = int(minutes_since_from_dt // 5)
            
            to_dt = from_dt + timedelta(minutes = candle_count * 5)
            from_dt = to_dt - timedelta(minutes=15)

            key = '-'.join([str(self.momentum_result['unique_key']), str(ce_token)])
            if key not in self.premium_data:
                self.premium_data.clear()
                ce_data = kite_login.conn.historical_data(ce_token, from_dt, to_dt, "5minute")
                self.premium_data[key] = pd.DataFrame(ce_data)

            ce_data_df = self.premium_data[key]
            
            if not ce_data_df.empty:
                parent_candle = self.historical_data_5m.iloc[-1]  # Last 5m candle
                ce_candle = ce_data_df.iloc[-1]  # Last CE candle
         
                stop_loss = ce_candle['low'] * 1.0 - 7.0
                target = round(ce_candle['low'], 2)
                
                ce_curr_data = live_data.get_current_data(ce_token)
                if ce_curr_data is None:
                    return None

                ce_price = ce_curr_data['price']

                if ce_candle['low'] < ce_price and float(ce_price) - float(stop_loss) <= 25.0:
                    ce_symbol = instrument_token.get_symbol_by_token(ce_token)
                    self.logging.info(f'CE BUY Order: ce_token - {ce_token}, ce_symbol - {ce_symbol}, ce_price - {ce_price}, stop_loss - {stop_loss}')
                    quantity = instrument_token.get_quantity(self.token)
                    if quantity is not None:
                        if self.low_margin_at is not None and datetime.now() - self.low_margin_at <= timedelta(minutes = 5):
                            return
                            
                        if self.get_fund(kite_login) + 1000 >  quantity * ce_price:
                            order = MarketOrder(ce_token, ce_symbol, self.logging)
                            order.update({'quantity': quantity})
                            order_id = order.buy_order(kite_login, stop_loss, target, ce_candle, scalping)
                            
                            if order_id is not None:
                                self.orders[ce_token] = order
                                self.last_order_key = self.momentum_result['unique_key']
                                self.logging.info(f"Order placed for {ce_symbol} with order id {order_id}")
                        else:
                            self.low_margin_at = datetime.now()
    
        except Exception as e:
            self.logging.error(f"Error in placing the CE order for {self.token}: {e}")
            self.logging.error(traceback.format_exc())

            
    def buy_pe_premium(self, kite_login, live_data, instrument_token, current_time, scalping):
        try:   
            pe_token = self.current_data_analysis['pe_token']

            from_dt = datetime(current_time.year, current_time.month, current_time.day, 9, 15)

            minutes_since_from_dt = (current_time - from_dt).total_seconds() // 60
            candle_count = int(minutes_since_from_dt // 5)
            
            to_dt = from_dt + timedelta(minutes = candle_count * 5)
            from_dt = to_dt - timedelta(minutes=15)

            key = '-'.join([str(self.momentum_result['unique_key']), str(pe_token)])
            if key not in self.premium_data:
                self.premium_data.clear()
                pe_data = kite_login.conn.historical_data(pe_token, from_dt, to_dt, "5minute")
                self.premium_data[key] = pd.DataFrame(pe_data)

            pe_data_df = self.premium_data[key]
            
            if not pe_data_df.empty:
                parent_candle = self.historical_data_5m.iloc[-1]  # Last 5m candle
                pe_candle = pe_data_df.iloc[-1]  # Last CE candle
  
                stop_loss = pe_candle['low'] * 1.0 - 7.0
                # target = round(pe_candle['high'] + 2 * parent_candle['ATR'] * 0.7, 2)
                target = round(pe_candle['low'], 2)

                pe_curr_data = live_data.get_current_data(pe_token)
                if pe_curr_data is None:
                    return None

                pe_price = pe_curr_data['price']
    
                if pe_candle['low'] < pe_price and float(pe_price) - float(stop_loss) <= 25.0:
                    pe_symbol = instrument_token.get_symbol_by_token(pe_token)
                    self.logging.info(f'PE BUY Order: pe_token - {pe_token}, pe_symbol - {pe_symbol}, pe_price - {pe_price}, stop_loss - {stop_loss}')
                    
                    quantity = instrument_token.get_quantity(self.token)
                    if quantity is not None:
                        if self.low_margin_at is not None and datetime.now() - self.low_margin_at <= timedelta(minutes = 5):
                            return
                            
                        if self.get_fund(kite_login) + 1000 >  quantity * pe_price:
                            order = MarketOrder(pe_token, pe_symbol, self.logging)
                            order.update({'quantity': quantity})
                            order_id = order.buy_order(kite_login, stop_loss, target, pe_candle, scalping)
                        
                            if order_id is not None:
                                self.orders[pe_token] = order
                                self.last_order_key = self.momentum_result['unique_key']
                                self.logging.info(f"Order placed for {pe_symbol} with order id {order_id}")
                        else:
                            self.low_margin_at = datetime.now()
    
        except Exception as e:
            self.logging.error(f"Error in placing the PE order for {self.token}: {e}")
            self.logging.error(traceback.format_exc())

        def sell_ce_premium(self, kite_login, live_data, instrument_token, current_time, scalping):
            try:   
                ce_token = self.current_data_analysis['ce_token']
      
                from_dt = datetime(current_time.year, current_time.month, current_time.day, 9, 15)
    
                minutes_since_from_dt = (current_time - from_dt).total_seconds() // 60
                candle_count = int(minutes_since_from_dt // 5)
                
                to_dt = from_dt + timedelta(minutes = candle_count * 5)
                from_dt = to_dt - timedelta(minutes=15)
    
                key = '-'.join([str(self.momentum_result['unique_key']), str(ce_token)])
                if key not in self.premium_data:
                    self.premium_data.clear()
                    ce_data = kite_login.conn.historical_data(ce_token, from_dt, to_dt, "5minute")
                    self.premium_data[key] = pd.DataFrame(ce_data)
    
                ce_data_df = self.premium_data[key]
                
                if not ce_data_df.empty:
                    parent_candle = self.historical_data_5m.iloc[-1]  # Last 5m candle
                    ce_candle = ce_data_df.iloc[-1]  # Last CE candle
                    
                    ce_curr_data = live_data.get_current_data(ce_token)
                    if ce_curr_data is None:
                        return None
    
                    ce_price = ce_curr_data['price']
    
                    ce_symbol = instrument_token.get_symbol_by_token(ce_token)
                    self.logging.info(f'CE SELL Order: ce_token - {ce_token}, ce_symbol - {ce_symbol}, ce_price - {ce_price}')
                    quantity = instrument_token.get_quantity(self.token)
                    if quantity is not None:
                        if self.low_margin_at is not None and datetime.now() - self.low_margin_at <= timedelta(minutes = 5):
                            return
                            
                        if self.get_fund(kite_login) + 1000 > 150000:
                            order = MarketOrder(ce_token, ce_symbol, self.logging)
                            order.update({'quantity': -quantity})
                            order_id = order.buy_order(kite_login, None, None, ce_candle, scalping)
                            
                            if order_id is not None:
                                self.orders[ce_token] = order
                                self.last_order_key = self.momentum_result['unique_key']
                                self.logging.info(f"Sell Order placed for {ce_symbol} with order id {order_id}")
                        else:
                            self.low_margin_at = datetime.now() 
                        
            except Exception as e:
                self.logging.error(f"Error in placing the CE order for {self.token}: {e}")
                self.logging.error(traceback.format_exc())

    def sell_pe_premium(self, kite_login, live_data, instrument_token, current_time, scalping):
        try:   
            pe_token = self.current_data_analysis['pe_token']

            from_dt = datetime(current_time.year, current_time.month, current_time.day, 9, 15)

            minutes_since_from_dt = (current_time - from_dt).total_seconds() // 60
            candle_count = int(minutes_since_from_dt // 5)
            
            to_dt = from_dt + timedelta(minutes = candle_count * 5)
            from_dt = to_dt - timedelta(minutes=15)

            key = '-'.join([str(self.momentum_result['unique_key']), str(pe_token)])
            if key not in self.premium_data:
                self.premium_data.clear()
                pe_data = kite_login.conn.historical_data(pe_token, from_dt, to_dt, "5minute")
                self.premium_data[key] = pd.DataFrame(pe_data)

            pe_data_df = self.premium_data[key]
            
            if not pe_data_df.empty:
                parent_candle = self.historical_data_5m.iloc[-1]  # Last 5m candle
                pe_candle = pe_data_df.iloc[-1]  # Last CE candle

                pe_curr_data = live_data.get_current_data(pe_token)
                if pe_curr_data is None:
                    return None

                pe_price = pe_curr_data['price']
    
                pe_symbol = instrument_token.get_symbol_by_token(pe_token)
                self.logging.info(f'PE SELL Order: pe_token - {pe_token}, pe_symbol - {pe_symbol}, pe_price - {pe_price}')
                
                quantity = instrument_token.get_quantity(self.token)
                if quantity is not None:
                    if self.low_margin_at is not None and datetime.now() - self.low_margin_at <= timedelta(minutes = 5):
                        return
                        
                    if self.get_fund(kite_login) + 1000 >  150000:
                        order = MarketOrder(pe_token, pe_symbol, self.logging)
                        order.update({'quantity': -quantity})
                        order_id = order.buy_order(kite_login, None, None, pe_candle, scalping)
                    
                        if order_id is not None:
                            self.orders[pe_token] = order
                            self.last_order_key = self.momentum_result['unique_key']
                            self.logging.info(f"Sell Order placed for {pe_symbol} with order id {order_id}")
                    else:
                        self.low_margin_at = datetime.now()
    
        except Exception as e:
            self.logging.error(f"Error in placing the PE order for {self.token}: {e}")
            self.logging.error(traceback.format_exc())
            
    def get_fund(self, kite_login):
        try:  
            funds = kite_login.conn.margins()
            return funds['equity']['available']['live_balance']
        except Exception as e:
            self.logging.error(f"Error in fetching funds: {e}")
            return 0

        
    def get_5m_candle_at(self, time):
        if self.historical_data_5m is not None:
            unique_key = Util.generate_5m_id(time)
            # Filter DataFrame for the row where unique_id matches
            result = self.historical_data_5m[self.historical_data_5m["unique_key"] == unique_key]
            return result.to_dict(orient="records")[0] if not result.empty else None

    def get_30m_candle_at(self, time):
        if self.historical_data_30m is not None:
            unique_key = Util.generate_30m_id(time)
            # Filter DataFrame for the row where unique_id matches
            result = self.historical_data_30m[self.historical_data_30m["unique_key"] == unique_key]
            return result.to_dict(orient="records")[0] if not result.empty else None

    def load_historical_test_data(self, unique_key):
        historical_data = HistoricalData(self.setting, self.token, self.logging)
        self.historical_data_5m  = historical_data.load_5min_data(unique_key)

    def get_ce_pe_tokens(self, instrument_token, token_data):
        if token_data is None:
            return None

        pe_ce_symbols = instrument_token.strike_price_tokens(self.token, token_data)
        if pe_ce_symbols is None or not pe_ce_symbols['ce_tokens'] or not pe_ce_symbols['pe_tokens']:
            return None


        return pe_ce_symbols

    def format_oi(self, value):
        if value >= 1_00_00_000:  # 1 crore
            return f"{value / 1_00_00_000:.2f} Cr"
        elif value >= 1_00_000:   # 1 lakh
            return f"{value / 1_00_000:.2f} L"
        else:
            return str(value)

    def load_trend_direction(self):
        if not self.historical_data_5m.empty and len(self.historical_data_5m) > 2:
            if self.market_trend['direction']:
                prev_candle = self.historical_data_5m.iloc[-2]
                current_candle = self.historical_data_5m.iloc[-1]
                open = current_candle['open']
                close = current_candle['close']
                body = abs(open - close)
                prev_color = "Green" if prev_candle['close'] > prev_candle['open'] else "Red"
                curr_color = "Green" if current_candle['close'] > current_candle['open'] else "Red"
    
                if curr_color == "Green" and body > 5:
                    self.market_trend['low'] = current_candle['low']
                elif curr_color == "Red" and body > 5:
                    self.market_trend['high'] = current_candle['high']

                self.market_trend['change'] = False
                if self.market_trend['direction'] == 'Up' and curr_color == "Red" and close + 0.5 < self.market_trend['low']:
                    self.market_trend['direction'] = 'Down'  # Fixed assignment
                    self.market_trend['change'] = True
                elif self.market_trend['direction'] == 'Down' and curr_color == "Green" and close > self.market_trend['high'] + 0.5:
                    self.market_trend['direction'] = 'Up'  # Fixed assignment
                    self.market_trend['change'] = True
            else:
                green_max_close = 0
                red_min_close = float('inf')  # Initialize to a very high value
    
                for i in range(len(self.historical_data_5m) - 1, 0, -1):  # Start from the most recent candle
                    prev_candle = self.historical_data_5m.iloc[i - 1]
                    current_candle = self.historical_data_5m.iloc[i]
    
                    if current_candle['close'] > current_candle['open']:  # Green candle
                        green_max_close = max(green_max_close, current_candle['close'])
                        red_min_close = float('inf')  # Reset red_min_close only here
                    else:  # Red candle
                        red_min_close = min(red_min_close, current_candle['close'])
                        green_max_close = 0  # Reset green_max_close
    
                    prev_color = "Green" if prev_candle['close'] > prev_candle['open'] else "Red"
                    curr_color = "Green" if current_candle['close'] > current_candle['open'] else "Red"
    
                    if self.market_trend['low'] is None and curr_color == 'Green':
                        self.market_trend['low'] = current_candle['low']
    
                    if self.market_trend['high'] is None and curr_color == 'Red':
                        self.market_trend['high'] = current_candle['high']
    
                    if curr_color != prev_color:  # Check trend change
                        if green_max_close > 0 and green_max_close > prev_candle['high'] + 0.5:
                            self.market_trend['direction'] = 'Up'
                        elif red_min_close < float('inf') and red_min_close + 0.5 < prev_candle['low']:
                            self.market_trend['direction'] = 'Down'
    
                    if self.market_trend['direction'] and self.market_trend['low'] and self.market_trend['high']:
                        break  # Stop the loop early if all values are set
    
        return None  # No trend reversal found


        
    def save_data_to_db(self, df):
        saved = False
        table_name = 'processed_details'

        columns_order = [
            'unique_key', 'date', 'trend', 'direction', 'signal', 'last_price', 'candle',
            'nearest_strike', 'nearest_pe_oi', 'nearest_ce_oi', 'nearest_pcr', 'nearest_gap',
            'next_strike', 'next_pe_oi', 'next_ce_oi', 'next_pcr', 'next_gap'
        ]
        
        df = df[columns_order]

        try:
            query = """INSERT INTO %s(unique_key, date, trend, direction, signal, last_price, candle,
                        nearest_strike, nearest_pe_oi, nearest_ce_oi, nearest_pcr, nearest_gap,
                        next_strike, next_pe_oi, next_ce_oi, next_pcr, next_gap) VALUES %%s""" % (table_name)
    
            # Ensure the database connection is established
            self.db_conn.connect()
    
            # Convert DataFrame to tuples for bulk insert
            tuples = [tuple(x) for x in df.to_numpy()]
    
            # Insert data using a bulk insert method
            self.db_conn.insert_bulk_data(query, tuples)
    
            # Commit transaction
            self.db_conn.commit()
            saved = True
        except Exception as e:
            self.logging.error(f"Error in saving processed data for {self.token} in processed_details: {e}")
        finally:
            if self.db_conn is not None:
                self.db_conn.close()
    
        return saved
        
        
            
    
        