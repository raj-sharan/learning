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
        self.orders       = {} 
        self.historical_data_5m = None
        self.historical_data_30m = None
        self.low_margin_at = None
        self.db_conn = PostgresDB(setting)
        self.processed_key = None

    def order_ids(self):
        return self.orders.keys()
        
    def load_historical_data(self):
        historical_data = HistoricalData(self.setting, self.token, self.logging)
        self.historical_data_5m  = historical_data.load_5min_data()
        self.historical_data_30m = historical_data.load_30min_data()

    def refresh_data(self, kite_login):
        current_time = datetime.now()
        # current_time = datetime(current_time.year, 2, 28, 14, 15)
        end_dt = datetime(current_time.year, current_time.month, current_time.day, 15, 30)
        if current_time > end_dt:
            current_time = end_dt
           
        if self.historical_data_5m is not None:
            from_dt = datetime(current_time.year, current_time.month, current_time.day, 9, 15)
            if self.refresh_till_5m is None: 
                self.refresh_till_5m = from_dt

            if current_time > self.refresh_till_5m and current_time - self.refresh_till_5m > timedelta(minutes = 5):
                minutes_since_from_dt = (current_time - from_dt).total_seconds() // 60
                candle_count = int(minutes_since_from_dt // 5)
            
                to_dt = from_dt + timedelta(minutes = candle_count * 5)
                if from_dt < to_dt:
                    try:
                        data = kite_login.conn.historical_data(self.token, from_dt, to_dt, "5minute")
                        data_df = pd.DataFrame(data)
     
                        if len(data_df) == candle_count:
                            data_df = data_df.drop(columns = ["volume"])
                            data_df['date'] = data_df.apply(lambda row: Util.parse_datetime(row["date"]), axis=1)
                            data_df["unique_key"] = data_df.apply(lambda row: Util.generate_5m_id(row["date"]), axis=1)
                            data_df["token"]      = data_df.apply(lambda row: self.token, axis = 1)
                            threshold = Util.generate_id(from_dt)
                            self.historical_data_5m.drop(self.historical_data_5m[self.historical_data_5m['unique_key'] >= threshold].index, inplace=True)
                            self.historical_data_5m = pd.concat([self.historical_data_5m, data_df], ignore_index=True)
                            self.historical_data_5m["SMA-200"] = moving_average_close_200sma(self.historical_data_5m)
                            self.historical_data_5m["SMA-20"] = moving_average_close_20sma(self.historical_data_5m)
                            self.historical_data_5m["ATR"] = atr(self.historical_data_5m)
                            self.historical_data_5m.drop(columns=['previous_close', 'high_low', 'high_close', 'low_close'], inplace=True)
                            self.refresh_till_5m = to_dt
                    except Exception as e:
                        self.logging.error(f"Error in refresh 5m data for {self.token}: {e}")

        current_time = datetime.now()
        end_dt = datetime(current_time.year, current_time.month, current_time.day, 15, 45)
        if current_time > end_dt:
            current_time = end_dt
            
        data_df = None
        if self.historical_data_30m is not None:
            from_dt = datetime(current_time.year, current_time.month, current_time.day, 9, 15)
            if self.refresh_till_30m is None:
                self.refresh_till_30m = from_dt
            
            if current_time > self.refresh_till_30m and current_time - self.refresh_till_30m > timedelta(minutes = 30):
                minutes_since_from_dt = (current_time - from_dt).total_seconds() // 60
                candle_count = int(minutes_since_from_dt // 30)
                
                to_dt = from_dt + timedelta(minutes = candle_count * 30)
                
                if from_dt < to_dt: 
                    try:
                        data = kite_login.conn.historical_data(self.token, from_dt, to_dt, "30minute")
                        data_df = pd.DataFrame(data)
                       
                        if len(data_df) == candle_count:
                            data_df = data_df.drop(columns = ["volume"])
                            data_df['date'] = data_df.apply(lambda row: Util.parse_datetime(row["date"]), axis=1)
                            data_df["unique_key"] = data_df.apply(lambda row: Util.generate_30m_id(row["date"]), axis=1)
                            data_df["token"]      = data_df.apply(lambda row: self.token, axis = 1)
                            threshold = Util.generate_id(from_dt)
                            self.historical_data_30m.drop(self.historical_data_30m[self.historical_data_30m['unique_key'] >= threshold].index, inplace=True)
                            self.historical_data_30m = pd.concat([self.historical_data_30m, data_df], ignore_index=True)
                            self.historical_data_30m["SMA-9-h"] = moving_average_high_9sma(self.historical_data_30m)
                            self.historical_data_30m["SMA-9-l"] = moving_average_low_9sma(self.historical_data_30m)
                            self.refresh_till_30m = to_dt
                    except Exception as e:
                        self.logging.error(f"Error in refresh 30m data for {self.token}: {e}")
                        # self.logging.error(traceback.format_exc())

    def execute_trade_opportunity(self, kite_login, live_data, instrument_token):
        if self.low_margin_at is not None and datetime.now() - self.low_margin_at <= timedelta(seconds = 5):
            return True
            
        current_time = datetime.now()
        # current_time = datetime(current_time.year, 2, 28, 14, 15)

        if self.refresh_till_5m is None:
        # if self.refresh_till_30m is None or self.refresh_till_5m is None:
            return

        # if current_time > self.refresh_till_30m and current_time - self.refresh_till_30m < timedelta(minutes=30):
            # candle_30m = self.historical_data_30m.iloc[-1]
            # if candle_30m["close"] > candle_30m["SMA-9-h"] + 3:
        candle_5m = self.historical_data_5m.iloc[-1]
        if candle_5m["SMA-20"] > candle_5m["SMA-200"] + 2 and candle_5m["low"] <= candle_5m["SMA-20"] + 7:
            pre_candle_5m = self.historical_data_5m.iloc[-2]
            if candle_5m["SMA-20"] > pre_candle_5m["SMA-20"] + 0.1:
                candle_set = self.historical_data_5m.tail(3)
                if valid_bulish_patterns(len(candle_set) - 1, candle_set):
                    self.buy_ce_premium(kite_login, live_data, instrument_token)
            # elif candle_30m["close"] < candle_30m["SMA-9-l"] - 3:
                # candle_5m = self.historical_data_5m.iloc[-1]
        elif candle_5m["SMA-20"] < candle_5m["SMA-200"] - 2 and candle_5m["high"] >= candle_5m["SMA-20"] - 7:
            pre_candle_5m = self.historical_data_5m.iloc[-2]
            if candle_5m["SMA-20"] < pre_candle_5m["SMA-20"] - 0.1:
                candle_set = self.historical_data_5m.tail(3)
                if valid_bearish_patterns(len(candle_set) - 1, candle_set):
                    self.buy_pe_premium(kite_login, live_data, instrument_token)
            

    def buy_ce_premium(self, kite_login, live_data, instrument_token):
        try:   
            token_data = live_data.get_current_data(self.token)
            
            if token_data is None:
                return None
                
            pe_ce_symbols = instrument_token.strike_price_tokens(self.token, token_data)
            if pe_ce_symbols is None or not pe_ce_symbols['ce_tokens']:
                return None

            token_price = token_data['price']
            ce_symbol = pe_ce_symbols['ce_tokens'][0]  # Get the first available CE token

            ce_token = instrument_token.get_token_by_symbol(ce_symbol)
            if ce_token is None:
                return None
            
            current_time = datetime.now()
            # current_time = datetime(current_time.year, 2, 28, 14, 15)
            from_dt = datetime(current_time.year, current_time.month, current_time.day, 9, 15)

            minutes_since_from_dt = (current_time - from_dt).total_seconds() // 60
            candle_count = int(minutes_since_from_dt // 5)
            
            to_dt = from_dt + timedelta(minutes = candle_count * 5)
            from_dt = to_dt - timedelta(minutes=15)

            ce_data = kite_login.conn.historical_data(ce_token, from_dt, to_dt, "5minute")
            ce_data_df = pd.DataFrame(ce_data)
            
            if not ce_data_df.empty:
                parent_candle = self.historical_data_5m.iloc[-1]  # Last 5m candle
                ce_candle = ce_data_df.iloc[-1]  # Last CE candle
         
                stop_loss = ce_candle['low'] - 5
                #target = round(ce_candle['high'] + 2 * parent_candle['ATR'] * 0.7, 2)
                target = round(ce_candle['low'] + parent_candle['ATR'], 2)
    
                ce_curr_data = live_data.get_current_data(ce_token)
                if ce_curr_data is None:
                    return None
                ce_price = ce_curr_data['price']

                if ce_candle['low'] < ce_price and ce_price - stop_loss <= 20:
                    self.logging.info(f'CE BUY Order: ce_token - {ce_token}, ce_symbol - {ce_symbol}, ce_price - {ce_price}, stop_loss - {stop_loss}, target - {target}')
                    self.logging.info(ce_candle)
                    
                    quantity = instrument_token.get_quantity(self.token)
                    if quantity is not None:
                        if self.get_fund(kite_login) + 1000 >  quantity * ce_price:
                            order = MarketOrder(ce_token, ce_symbol, self.logging)
                            order.update({'quantity': quantity})
                            order_id = order.buy_order(kite_login, stop_loss, target, ce_candle)
                            
                            if order_id is not None:
                                self.oredrs[ce_token] = order
                                self.logging.info(f"Order placed for {ce_symbol} with order id {order_id}")
                        else:
                            self.low_margin_at = datetime.now()
    
        except Exception as e:
            self.logging.error(f"Error in placing the CE order for {self.token}: {e}")

            
    def buy_pe_premium(self, kite_login, live_data, instrument_token):
        try:   
            token_data = live_data.get_current_data(self.token)
            
            if token_data is None:
                return None
            token_price = token_data['price']
            
            pe_ce_symbols = instrument_token.strike_price_tokens(self.token, token_data)
            
            if pe_ce_symbols is None or not pe_ce_symbols['pe_tokens']:
                return None
    
            pe_symbol = pe_ce_symbols['pe_tokens'][0]  # Get the first available CE token

            pe_token = instrument_token.get_token_by_symbol(pe_symbol)
            if pe_token is None:
                return None
            
            current_time = datetime.now()
            # current_time = datetime(current_time.year, 2, 28, 14, 15)
            from_dt = datetime(current_time.year, current_time.month, current_time.day, 9, 15)

            minutes_since_from_dt = (current_time - from_dt).total_seconds() // 60
            candle_count = int(minutes_since_from_dt // 5)
            
            to_dt = from_dt + timedelta(minutes = candle_count * 5)
            from_dt = to_dt - timedelta(minutes=15)

            pe_data = kite_login.conn.historical_data(pe_token, from_dt, to_dt, "5minute")
            pe_data_df = pd.DataFrame(pe_data)
       
            if not pe_data_df.empty:
                parent_candle = self.historical_data_5m.iloc[-1]  # Last 5m candle
                pe_candle = pe_data_df.iloc[-1]  # Last CE candle
  
                stop_loss = pe_candle['low'] - 5
                # target = round(pe_candle['high'] + 2 * parent_candle['ATR'] * 0.7, 2)
                target = round(pe_candle['low'] + parent_candle['ATR'], 2)
    
                pe_curr_data = live_data.get_current_data(pe_token)
                if pe_curr_data is None:
                    return None
                pe_price = pe_curr_data['price']
               
                if pe_candle['low'] < pe_price and pe_price - stop_loss <= 20:
                    self.logging.info(f'PE BUY Order: pe_token - {pe_token}, pe_symbol - {pe_symbol}, pe_price - {pe_price}, stop_loss - {stop_loss}, target - {target}')
                    self.logging.info(pe_candle)
                    
                    quantity = instrument_token.get_quantity(self.token)
                    if quantity is not None:
                        if self.get_fund(kite_login) + 1000 >  quantity * pe_price:
                            order = MarketOrder(pe_token, pe_symbol, self.logging)
                            order.update({'quantity': quantity})
                            order_id = order.buy_order(kite_login, stop_loss, target, pe_candle)
                        
                            if order_id is not None:
                                self.orders[pe_token] = order
                                self.logging.info(f"Order placed for {pe_symbol} with order id {order_id}")
                        else:
                            self.low_margin_at = datetime.now()
    
        except Exception as e:
            self.logging.error(f"Error in placing the CE order for {self.token}: {e}")
            # self.logging.error(traceback.format_exc())

    def execute_trade_opportunity_with_beta(self, kite_login, live_data, instrument_token):
        if self.low_margin_at is not None and datetime.now() - self.low_margin_at <= timedelta(minutes = 5):
            return True

        current_time = datetime.now()
        session_end_dt = datetime(current_time.year, current_time.month, current_time.day, 14, 55)
        if self.refresh_till_5m is None or current_time > session_end_dt:
            return
        
        if current_time > self.refresh_till_5m and current_time - self.refresh_till_5m < timedelta(minutes = 5):     
            candle_5m = self.historical_data_5m.iloc[-1]
            unique_key = Util.generate_5m_id(candle_5m['date'])
    
            sma_20 = candle_5m["SMA-20"]
            candle_set = self.historical_data_5m.tail(3)
            is_bulish = valid_bulish_patterns(len(candle_set) - 1, candle_set)
            is_bearish = valid_bearish_patterns(len(candle_set) - 1, candle_set)

            if is_bulish or is_bearish:
                token_data = live_data.get_current_data(self.token)
        
                if token_data is None:
                    return None
                token_price = token_data['price']
                
                pe_ce_symbols = instrument_token.strike_price_tokens(self.token, token_data)
                if pe_ce_symbols is None or not pe_ce_symbols['ce_tokens'] or not pe_ce_symbols['pe_tokens']:
                    return None
        
                ce_symbol = pe_ce_symbols['ce_tokens'][0]  # Get the first available CE token
                pe_symbol = pe_ce_symbols['pe_tokens'][0]  # Get the first available PE token
        
                ce_token = instrument_token.get_token_by_symbol(ce_symbol)
                pe_token = instrument_token.get_token_by_symbol(pe_symbol)
                if ce_token is None or pe_token is None:
                    return None
        
                momentums = live_data.analyser.analyse_momentum(candle_5m['date'], self.token, ce_token, pe_token)
        
                if not momentums:
                    return None
        
                print(momentums['beta'])
                print(momentums['corr'])
                result = {
                    'parent_token' : self.token,
                    'unique_key': unique_key,
                    'date': candle_5m['date'], 
                    'sma-20': sma_20,
                    'is_bulish': is_bulish,
                    'is_bearish': is_bearish,
                    'ce_token' : ce_token, 
                    'ce_beta' : momentums['beta']['ce_beta'].round(2) if 'beta' in momentums else 0.0, 
                    'ce_oi' : momentums['corr']['last_price']['oi_ce'].round(2)  if 'corr' in momentums else 0.0, 
                    'ce_quantity': momentums['corr']['last_price']['quantity_ce'].round(2)  if 'corr' in momentums else 0.0, 
                    'pe_token': pe_token, 
                    'pe_beta': momentums['beta']['pe_beta'].round(2)  if 'beta' in momentums else 0.0, 
                    'pe_oi': momentums['corr']['last_price']['oi_pe'].round(2)  if 'corr' in momentums else 0.0, 
                    'pe_quantity': momentums['corr']['last_price']['quantity_pe'].round(2)  if 'corr' in momentums else 0.0
                }

                if self.processed_key is None or unique_key > self.processed_key:
                    result_df = pd.DataFrame([result])
            
                    if self.save_data_to_db(result_df):
                        self.processed_key = unique_key
        
                pre_candle_5m = self.historical_data_5m.iloc[-2]
                if is_bulish and sma_20 > pre_candle_5m["SMA-20"] + 0.1 and candle_5m["low"] <= sma_20 + 10:
                    if abs(result['pe_beta']) > result['ce_beta']:
                        self.buy_ce_premium(kite_login, live_data, instrument_token)
        
                if is_bearish and sma_20 < pre_candle_5m["SMA-20"] - 0.1 and candle_5m["high"] >= sma_20 - 10:
                    if result['ce_beta'] > abs(result['pe_beta']):
                        self.buy_pe_premium(kite_login, live_data, instrument_token)
        
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

    def save_data_to_db(self, df):
        saved = False
        table_name = 'processed_details'
        try:
            query = """INSERT INTO %s(parent_token, unique_key, date, sma_20, is_bullish, is_bearish, 
                        ce_token, ce_beta, ce_oi, ce_quantity, pe_token, pe_beta, pe_oi, pe_quantity) VALUES %%s""" % (table_name)
    
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
        
   #  def manager_order(self, ksw, live_data):
   #      if self.oredrs.empty:
   #          if live_data.check_pattern(self.token) == True:
   #              token, name = get_strick_price(live_data.current(self.token))
   #              subscribe(ksw, True, token)
   #              candel = get_candel(kite, token, pattern_time)
   #              order = GttOrder.new(token, name)
   #              order.buy(candle, calculate_target(live_data.current(elf.token)), is_index)
   #              self.orders.add(order)
   #          else:
   #              for order in self.oredrs:
   #                  if !order.not_exists?
   #                      subscribe(ksw, True, order.token)
   #                  order.update(kite, live_data.current(order.token))
   #                  if order.not_exists?:
   #                      self.orders.delete(order)
   #                      subscribe(ksw, False, order.token)
                        
                    
                
   #  def calculate_target(self, current_data):
   #      data = self.historical_data_5m[-2,] + current_data
   #      atr = data - 0
      
   #      return atr

   #  def get_strick_price_token(self, current):
   #      strick_price = self.is_index ? int(current.ltp / 100) : int(current.ltp / 10)
   #      strick_price_details = kite.get_intruments(strick_price)
   #      {strick_price_details["token"], strick_price_details["name"])

   #  def get_candel(self, kite, token, time):
   #      data = kite.historical_data(stoken, Time.current - 10.mintutes, Time.current, 5)
   #      data[data["time"] == time]
        
   #  def subscribe(self, ksw, flag, token):
   #      if flag == True:
   #          kws.subscribe(token)
			# kws.set_mode(kws.MODE_LTP, token)
   #      else:
   #          kws.unsubscribe(token)
			# kws.set_mode(kws.MODE_LTP, token)
        
        
            
    
        