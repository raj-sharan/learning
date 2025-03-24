from datetime import datetime, timedelta
import pandas as pd
import traceback

from  moving_averages import *
from  candlestick_patterns import *
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
        self.ce_pe_token = {'ce_token': None, 'pe_token': None}
        self.reuse_tokens = False

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
                            self.historical_data_5m["SMA-50"] = moving_average_close_50sma(self.historical_data_5m)
                            self.historical_data_5m["SMA-20"] = moving_average_close_20sma(self.historical_data_5m)
                            self.historical_data_5m["SMA-15"] = moving_average_close_15sma(self.historical_data_5m)
                            self.historical_data_5m["SMA-9"] = moving_average_close_9sma(self.historical_data_5m)
                            # self.historical_data_5m["ATR"] = atr(self.historical_data_5m)
                            # self.historical_data_5m.drop(columns=['previous_close', 'high_low', 'high_close', 'low_close'], inplace=True)
                            self.refresh_till_5m = to_dt
                            self.logging.info(f"Refreshed 5m data for {self.token} till {self.refresh_till_5m}")
                    except Exception as e:
                        self.logging.error(f"Error in refresh 5m data for {self.token}: {e}")

        
        return # returning because following historical_data_30m are not being used
        # current_time = datetime.now()
        # end_dt = datetime(current_time.year, current_time.month, current_time.day, 15, 45)
        # if current_time > end_dt:
        #     current_time = end_dt
            
        # data_df = None
        # if self.historical_data_30m is not None:
        #     from_dt = datetime(current_time.year, current_time.month, current_time.day, 9, 15)
        #     if self.refresh_till_30m is None:
        #         self.refresh_till_30m = from_dt
            
        #     if current_time > self.refresh_till_30m and current_time - self.refresh_till_30m > timedelta(minutes = 30):
        #         minutes_since_from_dt = (current_time - from_dt).total_seconds() // 60
        #         candle_count = int(minutes_since_from_dt // 30)
                
        #         to_dt = from_dt + timedelta(minutes = candle_count * 30)
                
        #         if from_dt < to_dt: 
        #             try:
        #                 data = kite_login.conn.historical_data(self.token, from_dt, to_dt, "30minute")
        #                 data_df = pd.DataFrame(data)
                       
        #                 if len(data_df) == candle_count:
        #                     data_df = data_df.drop(columns = ["volume"])
        #                     data_df['date'] = data_df.apply(lambda row: Util.parse_datetime(row["date"]), axis=1)
        #                     data_df["unique_key"] = data_df.apply(lambda row: Util.generate_30m_id(row["date"]), axis=1)
        #                     data_df["token"]      = data_df.apply(lambda row: self.token, axis = 1)
        #                     threshold = Util.generate_id(from_dt)
        #                     self.historical_data_30m.drop(self.historical_data_30m[self.historical_data_30m['unique_key'] >= threshold].index, inplace=True)
        #                     self.historical_data_30m = pd.concat([self.historical_data_30m, data_df], ignore_index=True)
        #                     self.historical_data_30m["SMA-9-h"] = moving_average_high_9sma(self.historical_data_30m)
        #                     self.historical_data_30m["SMA-9-l"] = moving_average_low_9sma(self.historical_data_30m)
        #                     self.refresh_till_30m = to_dt
        #             except Exception as e:
        #                 self.logging.error(f"Error in refresh 30m data for {self.token}: {e}")
                        # self.logging.error(traceback.format_exc())

    def load_momentum_analysis(self, kite_login, live_data, instrument_token, current_time):
        if self.historical_data_5m.empty or "SMA-20" not in self.historical_data_5m:
            return 
            
        if self.refresh_till_5m and current_time - self.refresh_till_5m <= timedelta(minutes = 5):
            candle_5m = self.historical_data_5m.iloc[-1]
            unique_key = Util.generate_5m_id(candle_5m['date'])
            if not self.momentum_result or self.momentum_result['unique_key'] != unique_key:
                self.reuse_tokens = False
                candle_set = self.historical_data_5m.tail(3)
                is_bullish = valid_bulish_patterns(len(candle_set) - 1, candle_set)
                is_bearish = valid_bearish_patterns(len(candle_set) - 1, candle_set)
                
                sma_20 = candle_5m["SMA-20"]

                ce_token, pe_token = self.get_ce_pe_tokens(instrument_token, live_data)

                if ce_token is None or pe_token is None:
                    return None

                self.reuse_tokens = True
                
                momentums = live_data.analyser.analyse_momentum(candle_5m['date'], self.token, ce_token, pe_token)
                
                if not momentums:
                    return None

                self.momentum_result = {
                    'parent_token' : self.token,
                    'unique_key': unique_key,
                    'date': candle_5m['date'], 
                    'sma_20': sma_20,
                    'is_bullish': is_bullish,
                    'is_bearish': is_bearish,
                    'ce_token' : ce_token, 
                    'ce_beta' : round(momentums['beta']['ce_beta'], 2) if 'beta' in momentums else 0.0,
                    'ce_oi' : round(momentums['oi_data']['ce_oi'], 2)  if 'oi_data' in momentums else 0.0,
                    'ce_oi_change' : round(momentums['oi_data']['ce_oi_change'], 2) if 'oi_data' in momentums else 0.0,
                    'ce_volume': momentums['ce_volume'] if 'ce_volume' in momentums else 0,
                    'pe_token': pe_token, 
                    'pe_beta': round(momentums['beta']['pe_beta'], 2) if 'beta' in momentums else 0.0, 
                    'pe_oi': round(momentums['oi_data']['pe_oi'], 2) if 'oi_data' in momentums else 0.0,
                    'pe_oi_change': round(momentums['oi_data']['pe_oi_change'], 2)  if 'oi_data' in momentums else 0.0,
                    'pe_volume': momentums['pe_volume'] if 'pe_volume' in momentums else 0
                }

                
    def load_current_data_analysis(self, live_data, instrument_token):
        ce_oi = pe_oi = 0
        ce_token = pe_token = None
        unique_key = None
        if self.momentum_result:
            ce_oi = self.momentum_result['ce_oi']
            pe_oi = self.momentum_result['pe_oi']
            ce_token = self.momentum_result['ce_token']
            pe_token = self.momentum_result['pe_token']
            unique_key = self.momentum_result['unique_key']
            
        if ce_token is None or pe_token is None:     
            ce_token, pe_token = self.get_ce_pe_tokens(instrument_token, live_data)

        if ce_token and pe_token:     
            ce_token_data = live_data.get_current_data(ce_token)
            pe_token_data = live_data.get_current_data(pe_token)

            if ce_token_data and pe_token_data:
                self.current_data_analysis['unique_key'] = unique_key
                self.current_data_analysis['ce_price'] = ce_token_data['price']
                self.current_data_analysis['ce_curr_oi'] = ce_token_data['oi']
                # print('++++++++++++++++++++++++++CE')
                # print([float(ce_token_data['oi']), float(ce_oi), float(ce_token_data['oi']) - float(ce_oi)])
                self.current_data_analysis['ce_curr_oi_change'] = float(ce_token_data['oi']) - float(ce_oi) if float(ce_oi) > 0.0 else 0.0
                
                self.current_data_analysis['pe_price'] = pe_token_data['price']
                self.current_data_analysis['pe_curr_oi'] = pe_token_data['oi']
                # print('++++++++++++++++++++++++++PE')
                # print([float(pe_token_data['oi']), float(pe_oi), float(pe_token_data['oi']) - float(pe_oi)])
                self.current_data_analysis['pe_curr_oi_change'] = float(pe_token_data['oi']) - float(pe_oi) if float(pe_oi) > 0.0 else 0.0

                self.current_data_analysis['ce_pe_oi_ratio'] = round(ce_token_data['oi'] / pe_token_data['oi'], 2) if pe_token_data['oi'] != 0 else 0.0
               

    def print_analysis_details(self, should_save = True):
        # Create DataFrame with the desired columns
        print_df = pd.DataFrame(columns=['Property', '|', 'CE', '|','PE'])
        print_df.loc[len(print_df)] = ["----------------", '|', "------", '|', "------"]

        # Add rows from momentum_result
        if self.momentum_result:
            bullish = 'Bullish' if self.momentum_result['is_bullish'] else '-'
            bearish = 'Bearish' if self.momentum_result['is_bearish'] else '-'
            print_df.loc[len(print_df)] = ["Candle Pattern", '|', bullish, '|', bearish]
            print_df.loc[len(print_df)] = ["CE-PE Beta", '|', self.momentum_result['ce_beta'], '|', self.momentum_result['pe_beta']]
            
        # Add rows from current_data_analysis
        if self.current_data_analysis:
            print_df.loc[len(print_df)] = ["Curr OI Change", '|', self.current_data_analysis['ce_curr_oi_change'], '|', self.current_data_analysis['pe_curr_oi_change']]
            print_df.loc[len(print_df)] = ["OI Ratio", '|', self.current_data_analysis['ce_pe_oi_ratio'], '|', '-']

        print_df.loc[len(print_df)] = ["----------------", '|', "------", '|', "------"]
        # Display the DataFrame nicely
        print(print_df)
            
        if not self.historical_data_5m.empty and self.momentum_result:
            candle_5m = self.historical_data_5m.iloc[-1]
            direction = 'NA'
            if candle_5m['close'] > candle_5m['open'] + 5:
                direction = 'UP'
                if abs(self.momentum_result['pe_beta']) > abs(self.momentum_result['ce_beta']):
                    direction = '✅ CE'
                    
            elif candle_5m['open'] > candle_5m['close'] + 5:
                direction = 'DOWN'
                if abs(self.momentum_result['ce_beta']) > abs(self.momentum_result['pe_beta']):
                    direction = '❌ PE'
            
            print(f'Signal : {direction} candle at {candle_5m['date']}')
        
    def execute_trade_opportunity(self, kite_login, live_data, instrument_token, current_time):     
        if self.refresh_till_5m is None or not self.momentum_result or not self.current_data_analysis:
            return
                
        if current_time > self.refresh_till_5m and current_time - self.refresh_till_5m < timedelta(minutes = 5):
            candle_5m = self.historical_data_5m.iloc[-1]
            
            check_unique_key = (
                candle_5m['unique_key'] == self.momentum_result['unique_key']
                and candle_5m['unique_key'] == self.current_data_analysis['unique_key']
            )
            
            if check_unique_key: 
                candle_5m = self.historical_data_5m.iloc[-1]
                body = candle_5m['close'] - candle_5m['open']
        
                sma_200 = candle_5m["SMA-200"]
                sma_50 = candle_5m["SMA-50"]
                sma_20 = candle_5m["SMA-20"]
                sma_15 = candle_5m["SMA-15"]
                sma_9 = candle_5m["SMA-9"]
                pre_candle_5m = self.historical_data_5m.iloc[-2]
                is_bullish = self.momentum_result['is_bullish']
                is_bearish = self.momentum_result['is_bearish']
                if is_bullish: #or abs(self.momentum_result['pe_beta']) > abs(self.momentum_result['ce_beta']): 
                    on_line = sma_200 + 3 >= candle_5m["low"] or sma_50 + 3 >= candle_5m["low"]
                    on_line = on_line or sma_20 + 3 >= candle_5m["low"] or sma_15 + 3 >= candle_5m["low"] or sma_9 + 3 >= candle_5m["low"]
                    if on_line and body > 5 and abs(self.momentum_result['pe_beta']) > abs(self.momentum_result['ce_beta']):
                        ce_oi_change = self.current_data_analysis['ce_curr_oi_change']
                        pe_oi_change = self.current_data_analysis['pe_curr_oi_change']
                        if (ce_oi_change > 0.0 and pe_oi_change < 0.0) or (ce_oi_change < 0.0 and pe_oi_change > 0.0):
                            print(f'✅ Buy CE (Bullish) at {candle_5m['date']}')
                            self.buy_premium('CE', kite_login, live_data, instrument_token, current_time)
                        elif (ce_oi_change > 0.0 and pe_oi_change > 0.0) or (ce_oi_change < 0.0 and pe_oi_change < 0.0):
                            if self.current_data_analysis['ce_pe_oi_ratio'] >= 2.5:
                                print(f'✅ Buy CE (Bullish) at {candle_5m['date']}')
                                self.buy_premium('CE', kite_login, live_data, instrument_token, current_time)
                            elif self.current_data_analysis['ce_pe_oi_ratio'] > .7:
                                print(f'✅ Buy CE (Scalping) at {candle_5m['date']}')
                                self.buy_premium('CE', kite_login, live_data, instrument_token, current_time, True)
                
                if is_bearish: #or abs(self.momentum_result['ce_beta']) > abs(self.momentum_result['pe_beta']):
                    on_line = sma_200 - 3 <= candle_5m["high"] or sma_50 - 3 <= candle_5m["high"]
                    on_line = on_line or sma_20 - 3 <= candle_5m["high"] or sma_15 - 3 <= candle_5m["high"] or sma_9 - 3 <= candle_5m["high"]
                    if on_line and body < -5 and abs(self.momentum_result['ce_beta']) > abs(self.momentum_result['pe_beta']):
                        ce_oi_change = self.current_data_analysis['ce_curr_oi_change']
                        pe_oi_change = self.current_data_analysis['pe_curr_oi_change']
                        if (ce_oi_change > 0.0 and pe_oi_change < 0.0) or (ce_oi_change < 0.0 and pe_oi_change > 0.0):
                            print(f'❌ Buy PE (Bearish) at {candle_5m['date']}')
                            self.buy_premium('PE', kite_login, live_data, instrument_token, current_time)
                        elif (ce_oi_change > 0.0 and pe_oi_change > 0.0) or (ce_oi_change < 0.0 and pe_oi_change < 0.0):
                            if self.current_data_analysis['ce_pe_oi_ratio'] <= .7:
                                print(f'❌ Buy PE (Bearish) at {candle_5m['date']}')
                                self.buy_premium('PE', kite_login, live_data, instrument_token, current_time)
                            elif self.current_data_analysis['ce_pe_oi_ratio'] < 2.5:
                                print(f'❌ Buy PE (Scalping) at {candle_5m['date']}')
                                self.buy_premium('PE', kite_login, live_data, instrument_token, current_time, True)

    def buy_premium(self, ce_pe, kite_login, live_data, instrument_token, current_time, scalping = False):
        session_end_dt = datetime(current_time.year, current_time.month, current_time.day, 14, 55)
        if current_time > session_end_dt:
            return
        
        if ce_pe == 'CE':
            self.buy_ce_premium(kite_login, live_data, instrument_token, current_time, scalping)
        elif ce_pe == 'PE':
            self.buy_pe_premium(kite_login, live_data, instrument_token, current_time, scalping)
            
    def buy_ce_premium(self, kite_login, live_data, instrument_token, current_time, scalping):
        try:   
            ce_token = self.momentum_result['ce_token']
  
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
         
                stop_loss = ce_candle['low'] * 1.0 - 5.0
                target = round(ce_candle['low'], 2)
                
                ce_curr_data = live_data.get_current_data(ce_token)
                if ce_curr_data is None:
                    return None

                ce_price = ce_curr_data['price']

                if ce_candle['low'] < ce_price and float(ce_price) - float(stop_loss) <= 20.0:
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
                                self.logging.info(f"Order placed for {ce_symbol} with order id {order_id}")
                        else:
                            self.low_margin_at = datetime.now()
    
        except Exception as e:
            self.logging.error(f"Error in placing the CE order for {self.token}: {e}")
            self.logging.error(traceback.format_exc())

            
    def buy_pe_premium(self, kite_login, live_data, instrument_token, current_time, scalping):
        try:   
            pe_token = self.momentum_result['pe_token']

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
  
                stop_loss = pe_candle['low'] * 1.0 - 5.0
                # target = round(pe_candle['high'] + 2 * parent_candle['ATR'] * 0.7, 2)
                target = round(pe_candle['low'], 2)

                pe_curr_data = live_data.get_current_data(pe_token)
                if pe_curr_data is None:
                    return None

                pe_price = pe_curr_data['price']
    
                if pe_candle['low'] < pe_price and float(pe_price) - float(stop_loss) <= 20.0:
                    pe_symbol = instrument_token.get_symbol_by_token(pe_token)
                    self.logging.info(f'PE BUY Order: pe_token - {pe_token}, pe_symbol - {pe_symbol}, pe_price - {pe_price}, stop_loss - {stop_loss}')
                    self.logging.info(pe_candle)
                    
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
                                self.logging.info(f"Order placed for {pe_symbol} with order id {order_id}")
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

    def load_historical_test_data(self):
        historical_data = HistoricalData(self.setting, self.token, self.logging)
        self.historical_data_5m  = historical_data.load_5min_data()

    def get_ce_pe_tokens(self, instrument_token, live_data):
        if self.ce_pe_token['ce_token'] and self.ce_pe_token['pe_token'] and (self.reuse_tokens or self.orders):
            return self.ce_pe_token['ce_token'], self.ce_pe_token['pe_token']
            
        token_data = live_data.get_current_data(self.token)

        if token_data is None:
            return None, None

        pe_ce_symbols = instrument_token.strike_price_tokens(self.token, token_data)
        if pe_ce_symbols is None or not pe_ce_symbols['ce_tokens'] or not pe_ce_symbols['pe_tokens']:
            return None, None

        ce_symbol = pe_ce_symbols['ce_tokens'][0]  # Get the first available CE token
        pe_symbol = pe_ce_symbols['pe_tokens'][0]  # Get the first available PE token

        ce_token = instrument_token.get_token_by_symbol(ce_symbol)
        pe_token = instrument_token.get_token_by_symbol(pe_symbol)
        self.ce_pe_token['ce_token'] = ce_token
        self.ce_pe_token['pe_token'] = pe_token

        return ce_token, pe_token
        
    def save_data_to_db(self, df):
        saved = False
        table_name = 'processed_details'

        columns_order = [
            'parent_token', 'unique_key', 'date', 'sma_20', 'is_bullish', 'is_bearish',
            'ce_token', 'ce_beta', 'ce_oi', 'ce_quantity', 
            'pe_token', 'pe_beta', 'pe_oi', 'pe_quantity', 
            'curr_oi', 'curr_volume', 'volume', 'curr_quantities'
        ]
        
        df = df[columns_order]
        
        try:
            query = """INSERT INTO %s(parent_token, unique_key, date, sma_20, is_bullish, is_bearish, 
                        ce_token, ce_beta, ce_oi, ce_quantity, pe_token, pe_beta, pe_oi, pe_quantity, 
                        curr_oi, curr_volume, volume, curr_quantities) VALUES %%s""" % (table_name)
    
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
        
        
            
    
        