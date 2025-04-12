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
                 # Generate a unique key (not used yet in the DataFrame)
                unique_key = Util.generate_5m_id(timestamp)
    
                token = tick['instrument_token']
                last_price = tick['last_price']
                oi = tick['oi'] if 'oi' in tick else 0
                volume_traded = tick['volume_traded'] if 'volume_traded' in tick else 0
                
                bid_volume, offer_volume = self.fetch_bid_offer_volume(tick)
                time = datetime(timestamp.year, timestamp.month, timestamp.day, timestamp.hour, timestamp.minute, timestamp.second)
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

    def calculate_oi_change(self, ce_token, pe_token, date):
        result = {"ce_oi_change": 0.0, "pe_oi_change": 0.0, 'ce_oi': 0.0, 'pe_oi': 0.0}
        
        # Fetch CE and PE OI data for the given date
        oi_ce_df = self.fetch_oi_records(ce_token, date)
        oi_pe_df = self.fetch_oi_records(pe_token, date)
        
        # Return defaults if data is missing
        if oi_ce_df is None or oi_pe_df is None or oi_ce_df.empty or oi_pe_df.empty:
            return result
    
        try:
            # print(oi_ce_df["oi"])
            # if len(oi_ce_df["oi"]) >= 2:
            #     print([oi_ce_df["oi"].iloc[-1], oi_ce_df["oi"].iloc[-2], oi_ce_df["oi"].iloc[-1] - oi_ce_df["oi"].iloc[-2]])
            # print('-------------------------')
            # print(oi_pe_df["oi"])
            # if len(oi_pe_df["oi"]) >= 2:
            #     print([oi_pe_df["oi"].iloc[-1], oi_pe_df["oi"].iloc[-2], oi_pe_df["oi"].iloc[-1] - oi_pe_df["oi"].iloc[-2]])
            # Calculate the change in OI (last value - previous value)
        
            result["ce_oi"] = oi_ce_df["oi"].iloc[-1]
            result["pe_oi"] = oi_pe_df["oi"].iloc[-1]
            if len(oi_ce_df["oi"]) >= 2:
                result["ce_oi_change"] = oi_ce_df["oi"].diff().iloc[-1]
            if len(oi_pe_df["oi"]) >= 2:
                result["pe_oi_change"] = oi_pe_df["oi"].diff().iloc[-1]
    
        except Exception as e:
            self.logging.error(f"Error calculating OI change: {e}")
    
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

    def fetch_oi_records(self, token, timestamp):
        try:
            time = datetime(timestamp.year, timestamp.month, timestamp.day, 9, 15, 0)
            from_unique_key = Util.generate_5m_id(time)
            to_unique_key = Util.generate_5m_id(timestamp)
            
            sql = f"""
                SELECT oi  
                FROM tick_details  
                WHERE unique_key >= {from_unique_key} AND unique_key <= {to_unique_key} AND token = {token}
                GROUP BY oi  
                ORDER BY MIN(created_at) ASC;
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
    