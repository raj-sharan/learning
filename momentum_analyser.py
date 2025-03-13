from datetime import datetime, timedelta
import pandas as pd

from common import Util
from db_connect import PostgresDB

class MomentumAnalyser:
    def __init__(self, setting, logging):
        self.current_data_df = pd.DataFrame(columns=['token', 'unique_key', 'date', 'last_price', 'oi', 'quantity'])
        self.db_conn = PostgresDB(setting)
        self.logging = logging

    def load_current_data(self, live_data):
        ticks_data = live_data.ticks_data.copy()
        
        for tick in ticks_data:
            if 'exchange_timestamp' not in tick:
                self.logging.error("exchange_timestamp missing")
                print(tick)
                continue
                
            timestamp = tick['exchange_timestamp']
             # Generate a unique key (not used yet in the DataFrame)
            unique_key = Util.generate_5m_id(timestamp)

            token = tick['instrument_token']
            last_price = tick['last_price']
            oi = tick['oi'] if 'oi' in tick else 0
            volume_traded = tick['volume_traded'] if 'volume_traded' in tick else 0
            
            last_traded_quantity = self.calculate_last_traded_quantity(tick)
            time = datetime(timestamp.year, timestamp.month, timestamp.day, timestamp.hour, timestamp.minute, timestamp.second)
            # Create DataFrame with a single row
            df = pd.DataFrame([{
                'token': token,
                'unique_key': unique_key,
                'date': time,
                'last_price': last_price,
                'oi': oi,
                'volume_traded': volume_traded,
                'quantity': last_traded_quantity
            }])

            self.save_data_to_db(df)

            # self.current_data_df = pd.concat([self.current_data_df, df], ignore_index=True)
            
        del live_data.ticks_data[0:len(ticks_data)]
        ticks_data.clear()
        
        # time = datetime.now() - timedelta(minutes = 10)
        # unique_key = Util.generate_5m_id(time)
        # self.current_data_df.drop(self.current_data_df[self.current_data_df['unique_key'] < unique_key].index, inplace=True)

        
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
        result["corr"] = self.calculate_correlation(merged_df)
    
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

    def calculate_correlation(self, merged_df):
        # Select relevant columns for correlation
        correlation_matrix = merged_df[
            ["last_price", "oi_ce", "quantity_ce", "oi_pe", "quantity_pe"]
        ].corr()

        return correlation_matrix

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
            last_traded_quantity = tick['last_traded_quantity']

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
            query = """INSERT INTO %s(token, unique_key, date, last_price, oi, volume_traded, quantity) VALUES %%s""" % (table_name)
    
            # Ensure the database connection is established
            self.db_conn.connect()
    
            # Convert DataFrame to tuples for bulk insert
            tuples = [tuple(x) for x in df.to_numpy()]
    
            # Insert data using a bulk insert method
            if not self.db_conn.insert_bulk_data(query, tuples):
                print(df)
    
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
                SELECT token, unique_key, date, last_price, oi, quantity 
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


     

    