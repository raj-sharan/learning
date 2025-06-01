from datetime import datetime, timedelta
import pandas as pd
import traceback

from common import Util
from db_connect import PostgresDB

class MomentumAnalyser:
    def __init__(self, setting, logging):
        self.ticks_data = []
        self.current_data_df = pd.DataFrame(columns=['token', 'unique_key', 'date', 'last_price', 'oi', 'quantity'])
        self.db_conn = PostgresDB(setting, logging)
        self.logging = logging

    def load_ticks(self, ticks):
        self.ticks_data.extend(ticks)
            
        
    def load_current_data(self, current_time, should_save = True):
        ticks_data = self.ticks_data.copy()

        if self.trading_windows(current_time):
            for tick in ticks_data:
                if 'exchange_timestamp' not in tick:
                    self.logging.error("exchange_timestamp missing")
                    continue
                    
                timestamp = tick['exchange_timestamp']
                
                token = tick['instrument_token']
                last_price = tick['last_price']
                oi = tick['oi'] if 'oi' in tick else 0
                volume_traded = tick['volume_traded'] if 'volume_traded' in tick else 0
                
                bid_volume, offer_volume = self.fetch_bid_offer_volume(tick)
                time = datetime(timestamp.year, timestamp.month, timestamp.day, timestamp.hour, timestamp.minute, timestamp.second)

                # Generate a unique key (not used yet in the DataFrame)
                unique_key = Util.generate_5m_id(time)
                
                # Create DataFrame with a single row
                df = pd.DataFrame([{
                    'token': token,
                    'unique_key': unique_key,
                    'date': time,
                    'last_price': last_price,
                    'oi': oi,
                    'volume_traded': volume_traded,
                    'bid_volume': bid_volume,
                    'offer_volume': offer_volume
                }])
    
                if should_save:
                    self.save_data_to_db(df)
    
                # self.current_data_df = pd.concat([self.current_data_df, df], ignore_index=True)
            
        del self.ticks_data[0:len(ticks_data)]
        ticks_data.clear()

    def analyse_oi_momentum(self, instrument_token, live_data, order_tokens):
        order_strikes = []
        instruments_data = live_data.instruments_data
        selected_tokens = instrument_token.selected_tokens
        if order_tokens:
            order_strikes = instrument_token.get_strike_price(order_tokens)
        
        if selected_tokens is None or not selected_tokens['token_list']:
            return None

        nearest_pre_strike = selected_tokens.get('nearest_pre_strike', -1)
        nearest_strike = selected_tokens.get('nearest_strike', -1)
        next_strike = selected_tokens.get('next_strike', -1)
        next_pre_strike = selected_tokens.get('next_pre_strike', -1)

        ce_pe_pre_nearest = selected_tokens.get(nearest_pre_strike, {'PE': {}, 'CE': {}})
        ce_pe_nearest = selected_tokens.get(nearest_strike, {'PE': {}, 'CE': {}})
        ce_pe_next = selected_tokens.get(next_strike, {'PE': {}, 'CE': {}})
        ce_pe_pre_next = selected_tokens.get(next_pre_strike, {'PE': {}, 'CE': {}})

        order_strike = order_strikes[0] if order_strikes else None 
        ce_pe_order = selected_tokens.get(order_strike, {'PE': {}, 'CE': {}})

        nearest_pre_ce_oi = nearest_pre_pe_oi = next_pre_ce_oi = next_pre_pe_oi = 0.0
        nearest_ce_oi = nearest_pe_oi = next_ce_oi = next_pe_oi = order_ce_oi = order_pe_oi = 0.0
        
        if ce_pe_pre_nearest['PE'] and ce_pe_pre_nearest['CE']:  
            nearest_pre_ce_data = live_data.get_current_data(ce_pe_pre_nearest['CE']['instrument_token'])
            nearest_pre_pe_data = live_data.get_current_data(ce_pe_pre_nearest['PE']['instrument_token'])
            nearest_pre_ce_oi = nearest_pre_ce_data['oi'] if nearest_pre_ce_data else 0.0
            nearest_pre_pe_oi = nearest_pre_pe_data['oi'] if nearest_pre_pe_data else 0.0
            
        if ce_pe_nearest['PE'] and ce_pe_nearest['CE']:  
            nearest_ce_data = live_data.get_current_data(ce_pe_nearest['CE']['instrument_token'])
            nearest_pe_data = live_data.get_current_data(ce_pe_nearest['PE']['instrument_token'])
            nearest_ce_oi = nearest_ce_data['oi'] if nearest_ce_data else 0.0
            nearest_pe_oi = nearest_pe_data['oi'] if nearest_pe_data else 0.0

        if ce_pe_next['PE'] and ce_pe_next['CE']:  
            next_ce_data = live_data.get_current_data(ce_pe_next['CE']['instrument_token'])
            next_pe_data = live_data.get_current_data(ce_pe_next['PE']['instrument_token'])
            next_ce_oi = next_ce_data['oi'] if next_ce_data else 0.0
            next_pe_oi = next_pe_data['oi'] if next_pe_data else 0.0

        if ce_pe_pre_next['PE'] and ce_pe_pre_next['CE']:  
            next_pre_ce_data = live_data.get_current_data(ce_pe_pre_next['CE']['instrument_token'])
            next_pre_pe_data = live_data.get_current_data(ce_pe_pre_next['PE']['instrument_token'])
            next_pre_ce_oi = next_pre_ce_data['oi'] if next_pre_ce_data else 0.0
            next_pre_pe_oi = next_pre_pe_data['oi'] if next_pre_pe_data else 0.0

        if ce_pe_order['PE'] and ce_pe_order['CE']:  
            order_ce_data = live_data.get_current_data(ce_pe_order['CE']['instrument_token'])
            order_pe_data = live_data.get_current_data(ce_pe_order['PE']['instrument_token'])
            order_ce_oi = order_ce_data['oi'] if order_ce_data else 0.0
            order_pe_oi = order_pe_data['oi'] if order_pe_data else 0.0

        pcr_pre_nearest = float(nearest_pre_pe_oi) / float(nearest_pre_ce_oi) if float(nearest_pre_ce_oi) != 0 else 0.0
        pcr_nearest = float(nearest_pe_oi) / float(nearest_ce_oi) if float(nearest_ce_oi) != 0 else 0.0
        pcr_next = float(next_pe_oi) / float(next_ce_oi) if float(next_ce_oi) != 0 else 0.0
        pcr_pre_next = float(next_pre_pe_oi) / float(next_pre_ce_oi) if float(next_pre_ce_oi) != 0 else 0.0
        pcr_order = float(order_pe_oi) / float(order_ce_oi) if float(order_ce_oi) != 0 else 0.0

        return {
            'pcr_pre_nearest': pcr_pre_nearest,
            'pcr_nearest': pcr_nearest,
            'pcr_next': pcr_next,
            'pcr_pre_next': pcr_pre_next,
            'oi_pre_nearest': [nearest_pre_pe_oi, nearest_pre_ce_oi],
            'oi_nearest': [nearest_pe_oi, nearest_ce_oi],
            'oi_next': [next_pe_oi, next_ce_oi],
            'oi_pre_next': [next_pre_pe_oi, next_pre_ce_oi],
            'strike_range': [nearest_strike, next_strike],
            'pre_strike_range': [nearest_pre_strike, next_pre_strike],
            'pcr_order': pcr_order
        }

        
    def analyse_momentum(self, date, parent_token, ce_token, pe_token, unique_key = None):
        result = {}
        if unique_key is None:
            unique_key = Util.generate_5m_id(date)

        current_data_df = self.fetch_records(parent_token, ce_token, pe_token, unique_key)
        
        if current_data_df is None or current_data_df.empty:  # Corrected
            return result

        if len(current_data_df) < 200:
            return result

        # Fetch DataFrames from stored data
        
        parent_df = current_data_df[(current_data_df['token'] == parent_token) & (current_data_df['unique_key'] == unique_key)]
        premium_ce_df = current_data_df[(current_data_df['token'] == ce_token) & (current_data_df['unique_key'] == unique_key)]
        premium_pe_df = current_data_df[(current_data_df['token'] == pe_token) & (current_data_df['unique_key'] == unique_key)]

        # Align data based on time (handles different row counts)
        merged_df = premium_ce_df.merge(premium_pe_df, on="date", suffixes=("_ce", "_pe"), how="inner")
        merged_df = merged_df.merge(parent_df, on="date", how="inner")
        
        # Drop rows where any required column is NaN
        merged_df = merged_df.dropna().drop_duplicates()

        if len(merged_df) < 50:
            return result
            
        # Calculate metrics
        result["beta"] = self.calculate_beta(merged_df)
        result["oi_data"] = self.calculate_oi_change(ce_token, pe_token, date)
        result["ce_volume"] = premium_ce_df['volume_traded'].iloc[-1]
        result["pe_volume"] = premium_pe_df['volume_traded'].iloc[-1]
    
        return result


    def calculate_beta(self, merged_df):
        # Calculate % Change
        merged_df["index_return"] = merged_df["last_price"].pct_change()
        merged_df["call_return"] = merged_df["last_price_ce"].pct_change()
        merged_df["put_return"] = merged_df["last_price_pe"].pct_change()

        # Compute Beta (Rate of Option Movement Relative to Index)
        ce_beta = merged_df["call_return"].cov(merged_df["index_return"]) / merged_df["index_return"].var()
        pe_beta = merged_df["put_return"].cov(merged_df["index_return"]) / merged_df["index_return"].var()

        return {"ce_beta": ce_beta, "pe_beta": pe_beta}

    def calculate_oi_change(self, date, instrument_tokens_data):
        result = {}

        tokens = instrument_tokens_data['token_list']
        ce_tokens = instrument_tokens_data['ce_tokens']
        pe_tokens = instrument_tokens_data['pe_tokens']
        # Fetch CE and PE OI data for the given date
        oi_df = self.fetch_oi_records(date, tokens)

        if oi_df is None or oi_df.empty:
            return result
    
        # Group by token and compute metrics
        grouped = oi_df.groupby("token")
        high_ce_oi = high_pe_oi = 0.0
        high_ce_oi_token = high_pe_oi_token = -1
        for token, group in grouped:
            filtered_group = group[group["oi"].diff() != 0].reset_index(drop=True)

            token_result = {
                "oi_change": 0.0,
                "oi_trend": ""
            }

            if len(filtered_group) >= 2:
                oi_trend = self.detect_oi_trend(filtered_group)
                token_result["oi_change"] = oi_trend['ptc']
                token_result["oi_trend"] = oi_trend['trend']

            if token in ce_tokens:
                oi = group["oi"].iloc[-1]
                if oi > high_ce_oi:
                    high_ce_oi = oi
                    high_ce_oi_token = token
            elif token in pe_tokens:
                oi = group["oi"].iloc[-1]
                if oi > high_pe_oi:
                    high_pe_oi = oi
                    high_pe_oi_token = token

            result[token] = token_result

            valid_ce_token = high_ce_oi_token != -1 and high_ce_oi_token in ce_tokens
            valid_pe_token = high_pe_oi_token != -1 and high_pe_oi_token in pe_tokens
            
            result['high_ce_oi_strike'] = {high_ce_oi_token: int(ce_tokens[high_ce_oi_token]["strike"])} if valid_ce_token else {}
            result['high_pe_oi_strike'] = {high_pe_oi_token: int(pe_tokens[high_pe_oi_token]["strike"])} if valid_pe_token else {}

        return result

    def detect_oi_trend(self, oi_data, window=7):
        result = {'trend': 'Unknown', 'ptc': 0.0}
        try:
            df = pd.DataFrame(oi_data)

            df['oi_sma'] = df['oi'].rolling(window=window, min_periods=1).mean()
    
            df['oi_sma_pct'] = df['oi_sma'].pct_change() * 100
            delta = df['oi_sma_pct'].iloc[-1]

            if delta > 0.4:
                result['trend'] = 'Increasing'
            elif delta < -0.4:
                result['trend'] = 'Decreasing'
            else:
                result['trend'] = 'Stable'

            result['ptc'] = delta

        except Exception as e:
            self.logging.error(f"Error detecting OI trend: {e}")
            
        return result

    def fetch_bid_offer_volume(self, tick):
        if 'depth' in tick:
            bid_volume = sum([level['quantity'] for level in tick['depth']['buy']])
            offer_volume = sum([level['quantity'] for level in tick['depth']['sell']])
            return bid_volume, offer_volume
        return 0, 0
  
    def calculate_last_traded_quantity(self, tick):
        last_traded_quantity = 0
        if 'depth' not in tick:
            return last_traded_quantity
            
        price = tick['last_price']
        highest_bid = max([bid['price'] for bid in tick['depth']['buy']])
        lowest_ask = min([ask['price'] for ask in tick['depth']['sell']])
        
        if price >= lowest_ask:  # If price reaches the lowest ask, it's a buy
            last_traded_quantity = tick['last_traded_quantity']
        elif price <= highest_bid:  # If price drops to the highest bid, it's a sell
            last_traded_quantity = - tick['last_traded_quantity']

        return last_traded_quantity

    def fetch_vi(self, kite_login, tokens):
        tokens_arr = [f'NFO:{item}' for item in tokens]
        option_details = kite_login.conn.quote(tokens_arr)  # Fetch IV
        result = {}
        for token in tokens:
            if f"NFO:{token}" in option_details:
                result[token] = option_details[f"NFO:{token}"]["implied_volatility"]
        
        return result

    def save_data_to_db(self, df):
        saved = False
        table_name = 'tick_details'
        try:
            query = """INSERT INTO %s(token, unique_key, date, last_price, oi, volume_traded, bid_volume, offer_volume) VALUES %%s""" % (table_name)
    
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

    def fetch_records(self, parent_token, ce_token, pe_token, unique_key):
        try:
            # Ensure ce_token and pe_token are lists and format them correctly
            token_list = [str(parent_token)] + [str(ce_token)] + [str(pe_token)]
            token_str = ",".join(token_list)  # Convert to comma-separated string

            sql = f"""
                SELECT token, unique_key, date, last_price, oi, volume_traded, bid_volume, offer_volume  
                FROM tick_details 
                WHERE unique_key = {unique_key} AND token IN ({token_str})
                ORDER BY created_at
            """
            
            self.db_conn.connect()
            return self.db_conn.get_records_in_data_frame(sql)  # Use self.db_conn instead of db
    
        except Exception as e:
            self.logging.error(f"Error fetching records for momentum calculation: {e}")
            return None  # Ensure None is returned on failure
    
        finally:
            if self.db_conn is not None:
                self.db_conn.close()  # Ensure connection is properly closed

    def fetch_oi_records(self, timestamp, tokens):
        try:
            time = datetime(timestamp.year, timestamp.month, timestamp.day, 9, 15, 0)
            from_unique_key = Util.generate_5m_id(time)
            to_unique_key = Util.generate_5m_id(timestamp)

            token_str = ','.join([str(item) for item in tokens])
            
            sql = f"""
                SELECT id, token, oi
                FROM tick_details
                WHERE token in ({token_str}) and unique_key >= {from_unique_key} AND unique_key <= {to_unique_key}
                ORDER BY token, id ASC;
            """
        
            self.db_conn.connect()
            
            return self.db_conn.get_records_in_data_frame(sql)  # Use self.db_conn instead of db
    
        except Exception as e:
            self.logging.error(f"Error fetching records for momentum calculation: {e}")
            return None  # Ensure None is returned on failure
    
        finally:
            if self.db_conn is not None:
                self.db_conn.close()  # Ensure connection is properly closed

    def fetch_ticks_data(self, from_dt, to_dt):
        try:
            sql = f"""
                SELECT token, date, last_price, oi, volume_traded, bid_volume, offer_volume  
                FROM tick_details 
                WHERE date >= '{from_dt}' AND date < '{to_dt}'
                ORDER BY created_at
            """
            
            self.db_conn.connect()
            ticks_data = self.db_conn.fetch_records(sql)  # Use self.db_conn instead of db
            ticks = []

            if len(ticks_data) > 0:     
                for row in ticks_data:
                    depth = {}
                    
                    # Only process rows with non-zero quantity
                    if row[5] != 0:
                        bid = abs(row[5]) if row[5] > 0 else 0
                        ask = abs(row[5]) if row[5] < 0 else 0
                        depth = {'buy': {'quantity': bid, 'price': 0}, 'sell': {'quantity': ask, 'price': 0}}
                
                    # Create the tick dictionary
                    tick = {
                        'instrument_token': row[0],
                        'exchange_timestamp': row[1],
                        'last_price': row[2],
                        'oi': row[3],
                        'volume_traded': row[4]
                    }
                
                    # Add depth data if present
                    if depth:
                        tick.update(depth)  # ✅ Use update to merge dictionaries
                
                    ticks.append(tick)  # ✅ Correct way to add items to a list
            
            return ticks

            
        except Exception as e:
            self.logging.error(f"Error fetching ticks_data: {e}")
            print(traceback.format_exc())
            return []  # Ensure None is returned on failure
        
        finally:
            if self.db_conn is not None:
                self.db_conn.close()  # Ensure connection is properly closed
        
    def trading_windows(self, current_time):
        from_dt = datetime(current_time.year, current_time.month, current_time.day, 9, 15)
        to_dt = datetime(current_time.year, current_time.month, current_time.day, 15, 30)
        return from_dt < current_time < to_dt
    