from datetime import datetime, timedelta
import pandas as pd
import traceback
import os

from common import Util
from db_connect import PostgresDB

class AiDataGenerator:

    def __init__(self, setting, logging):
        self.logging = logging
        self.setting = setting
        self.db_conn = PostgresDB(setting, logging)
        self.cache_instrument_tokens = None

    def ce_pe_oi_ratio(self, row):
        ce_oi = row['ce_curr_oi']
        pe_oi = row['pe_curr_oi']
        return round(ce_oi / pe_oi if pe_oi != 0 else 0.0, 4)

    def prev_ce_pe_oi_ratio(self, row):
        ce_oi = row['old_ce_oi']
        pe_oi = row['old_pe_oi']
        return round(ce_oi / pe_oi if pe_oi != 0 else 0.0, 4)

    def ce_oi_change(self, row):
        curr_ce_oi = row['ce_curr_oi']
        old_ce_oi = row['ce_oi']
        return round(curr_ce_oi / old_ce_oi if old_ce_oi != 0 else 0.0, 4)

    def pre_ce_oi_change(self, row):
        curr_ce_oi = row['ce_curr_oi']
        old_ce_oi = row['old_ce_oi']
        return round(curr_ce_oi / old_ce_oi if old_ce_oi != 0 else 0.0, 4)

    def pe_oi_change(self, row):
        curr_pe_oi = row['pe_curr_oi']
        old_pe_oi = row['pe_oi']
        return round(curr_pe_oi / old_pe_oi if old_pe_oi != 0 else 0.0, 4)

    def pre_pe_oi_change(self, row):
        curr_pe_oi = row['pe_curr_oi']
        old_pe_oi = row['old_pe_oi']
        return round(curr_pe_oi / old_pe_oi if old_pe_oi != 0 else 0.0, 4)

    def determine_action(self, row):
        return 'No Trade'

    def map_token_to_symbol(self, row):
        token = row['token']
        return self.cache_instrument_tokens[token]
        
        
    def generate_oi_difference(self, instrument_items):
        try:
            self.cache_instrument_tokens = instrument_items
            self.db_conn.connect()
            query = """SELECT token, date, oi, last_price FROM tick_details where oi != 0 order by token, date;"""
    
            data_df = self.db_conn.get_records_in_data_frame(query)
            
            data_df['symbol'] = data_df.apply(self.map_token_to_symbol, axis=1)
            data_df['oi_change'] = data_df['oi'].diff()
            data_df['oi_change_ratio'] = round(data_df['oi'].pct_change() * 100, 2)

            data_df = data_df[data_df['oi_change'] != 0.0]

            if not os.path.exists('oi_traning_data.csv'):
                data_df[['token', 'symbol', 'date', 'oi', 'oi_change', 'oi_change_ratio', 'last_price']].to_csv('oi_traning_data.csv', index=False)
            else:
                self.logging.error("Please the delete file 'traning_data.csv' first, Already exists")
        except Exception as e:
            self.logging.error(f"Error in processing data for traning: {e}")
            self.logging.error(traceback.format_exc())
            
        finally:
            if self.db_conn is not None:
                self.db_conn.close()

    def get_strike_id(self, row):
        nearest_strike = str(row['nearest_strike'])
        next_strike = str(row['next_strike'])
        return "-".join([nearest_strike, next_strike])

    def get_date_format(self, row):
        dt = row['date']
        if isinstance(row['date'], str):
            dt = datetime.strptime(row['date'], "%Y-%m-%d %H:%M:%S.%f")

        # Format to desired string
        return dt.strftime("%d/%m/%Y %H:%M:%S")

    def generate(self, current_date, end_date):
        data_table = 'traning_data'
    
        from_dt = datetime(current_date.year, current_date.month, current_date.day, 9, 15)
        to_dt   = datetime(end_date.year, end_date.month, end_date.day, 15, 30)
    
        try:
            from_unique_key = Util.generate_5m_id(from_dt)
            to_unique_key = Util.generate_5m_id(to_dt)
    
            self.db_conn.connect()
    
            query = """
                SELECT unique_key, date, trend, direction, signal, last_price, candle,
                       nearest_strike, nearest_pe_oi, nearest_ce_oi, nearest_pcr, nearest_gap,
                       next_strike, next_pe_oi, next_ce_oi, next_pcr, next_gap
                FROM %s
            """ % data_table
    
            data_df = self.db_conn.get_records_in_data_frame(query)
    
            # Add derived columns
            data_df['date_id'] = data_df.apply(lambda row: Util.generate_date_id(row["date"]), axis=1)
            data_df['strike_id'] = data_df.apply(self.get_strike_id, axis=1)
            data_df['time'] = data_df.apply(self.get_date_format, axis=1)
    
            # Sort before diff
            data_df.sort_values(by=['date_id', 'strike_id', 'time'], inplace=True)
    
            # Calculate OI differences grouped by date_id and strike_id
            oi_columns = ['nearest_pe_oi', 'nearest_ce_oi', 'next_pe_oi', 'next_ce_oi']
            for col in oi_columns:
                print(data_df.groupby(['date_id', 'strike_id'])[col])
                data_df[col + '_diff'] = data_df.groupby(['date_id', 'strike_id'])[col].diff()
    
            # Write to CSV if not exists
            output_file = 'traning_data.csv'
            if not os.path.exists(output_file):
                data_df.to_csv(output_file, index=False)
            else:
                self.logging.error(f"Please delete the file '{output_file}' first. It already exists.")
    
        except Exception as e:
            self.logging.error(f"Error in processing data for training: {e}")
            self.logging.error(traceback.format_exc())
    
        finally:
            if self.db_conn is not None:
                self.db_conn.close()


    def encode_action(self, row):
        if row['action'] == 'No Trade':
            return 0
        elif row['action'] == 'Buy':
            return 1
        elif row['action'] == 'Buy Hold':
            return 2
        elif row['action'] == 'Weak Buy':
            return 3
        elif row['action'] == 'Weak Buy Hold':
            return 4
        elif row['action'] == 'Sell':
            return 5
        elif row['action'] == 'Sell Hold':
            return 6
        elif row['action'] == 'Weak Sell':
            return 7
        elif row['action'] == 'Weak Sell Hold':
            return 8
        else:
            self.logging.error(f"Wrong value added in action column: {row['action']}")
            return -1

    def encode_state(self, row):
        if row['state'] == 'No Trade':
            return 0
        elif row['state'] == 'Buy':
            return 1
        elif row['state'] == 'Weak Buy':
            return 2
        elif row['state'] == 'Sell':
            return 3
        elif row['state'] == 'Weak Sell':
            return 4
        else:
            self.logging.error(f"Wrong value added in state column: {row['state']}")
            return -1
            
    def encode_direction(self, row):
        if row['direction'] == 'Up':
            return 1
        elif row['direction'] == 'Down':
            return 0
        else:
            self.logging.error(f"Wrong value added in direction column: {row['direction']}")
            return -1

    def correct_date_format(self, row):
        time = row['date']
        if isinstance(time, str):
            time = pd.to_datetime(time)

        formatted_date = time.strftime("%Y-%m-%d %H:%M:%S")
        
        return formatted_date
        
            
    def load_traning_data(self, csv):
        data_df = pd.read_csv(csv)

        data_df["date"] = data_df.apply(self.correct_date_format, axis=1)
        data_df["unique_key"] = data_df.apply(lambda row: Util.generate_5m_id(row["date"]), axis=1)
        data_df["direction"] = data_df.apply(self.encode_direction, axis=1)
        data_df["action"] = data_df.apply(self.encode_action, axis=1)
        data_df["state"] = data_df.apply(self.encode_state, axis=1)

        if self.save_data_to_db(data_df):
            self.logging.info("Saved traning data")
        else:
            self.logging.error("Failed to save traning data")
        
    def save_data_to_db(self, df):
        saved = False
        traning_data_table = 'traning_data'

        columns_order = [
            'unique_key', 'date', 'time', 'direction', 'ce_pe_oi_ratio', 'prev_ce_pe_oi_ratio',
            'ce_beta', 'ce_oi_change', 'pre_ce_oi_change',
            'pe_beta', 'pe_oi_change', 'pre_pe_oi_change', 'state', 'action'
        ]
        
        df = df[columns_order]

        
        try:
            query = """INSERT INTO %s(unique_key, date, time, direction, ce_pe_oi_ratio, prev_ce_pe_oi_ratio,
                        ce_beta, ce_oi_change, pre_ce_oi_change,
                        pe_beta, pe_oi_change, pre_pe_oi_change, state, action) VALUES %%s""" % (traning_data_table)
    
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

    def fetch_traning_data(self, from_date = None):
        data_table = 'traning_data'
        
        if from_date:
            from_dt = datetime(from_date.year, from_date.month, from_date.day, 9, 15)
        else:
            from_dt = datetime(2025, 1, 1, 9, 15)
        
        try:
            from_unique_key = Util.generate_5m_id(from_dt)
            
            self.db_conn.connect()
            query = """SELECT id, unique_key, date, time, direction, ce_pe_oi_ratio, prev_ce_pe_oi_ratio,
                        ce_beta, ce_oi_change, pre_ce_oi_change,
                        pe_beta, pe_oi_change, pre_pe_oi_change, state, action FROM %s
            where unique_key >= %s order by id
            """ % (data_table, from_unique_key)
            
            df = self.db_conn.get_records_in_data_frame(query)

            return df
        except Exception as e:
            self.logging.error(f"Error in processing data for traning: {e}")
        finally:
            if self.db_conn is not None:
                self.db_conn.close()
        
        return None

    def reset_date(self, row_id, new_date):
        data_table = 'traning_data'
        unique_key = Util.generate_5m_id(new_date)
        try:
            self.db_conn.connect()
            update_query = "UPDATE traning_data SET date = %s, unique_key = %s WHERE id = %s"
            values = (new_date, unique_key, row_id)

            self.db_conn.execute_query(update_query, values)
            
            self.db_conn.commit()
            return True
        except Exception as e:
            self.logging.error(f"Error in processing data for traning: {e}")
        finally:
            if self.db_conn is not None:
                self.db_conn.close()
        
        return False

    def copy_to_tick_details_copy(self):
        from_data_table = 'tick_details'
        to_data_table = 'tick_details_copy'
        
        try: 
            self.db_conn.connect()
            
            query = """SELECT * FROM %s ORDER BY id ASC""" % from_data_table
        
            data_df = self.db_conn.get_records_in_data_frame(query)
        
            self.logging.info(f"fetched Data length: {len(data_df)}")
         
            columns_order = ['token', 'unique_key', 'date', 'last_price', 'oi', 'volume_traded', 'bid_volume', 'offer_volume']
        
            sorted_df = data_df[columns_order]
        
            self.logging.info(f"Data length : {len(sorted_df)}")
        
            query = """INSERT INTO %s(token, unique_key, date, last_price, oi, volume_traded, bid_volume, offer_volume) VALUES %%s""" % (to_data_table)
        
            # Convert DataFrame to tuples for bulk insert
            tuples = [tuple(x) for x in sorted_df.to_numpy()]
        
            # Insert data using a bulk insert method
            self.db_conn.insert_bulk_data(query, tuples)
        
            # Commit transaction
            self.db_conn.commit()
        
            return True
        except Exception as e:
            self.logging.error(f"Error in processing data for traning: {e}")
        finally:
            if self.db_conn is not None:
                self.db_conn.close()
        
        return None
         
    def copy_to_traning_table(self):
        from_data_table = 'processed_details'
        to_data_table = 'traning_data'
        
        try: 
            self.db_conn.connect()
            
            query = """SELECT * FROM %s""" % from_data_table
    
            data_df = self.db_conn.get_records_in_data_frame(query)

            self.logging.info(f"fetched Data length: {len(data_df)}")

            columns_order = [
            'unique_key', 'date', 'trend', 'direction', 'signal', 'last_price', 'candle',
            'nearest_strike', 'nearest_pe_oi', 'nearest_ce_oi', 'nearest_pcr', 'nearest_gap',
            'next_strike', 'next_pe_oi', 'next_ce_oi', 'next_pcr', 'next_gap'
            ]
        
            sorted_df = data_df.sort_values(by=['date'])[columns_order]

            self.logging.info(f"Data length : {len(sorted_df)}")
    
            query = """INSERT INTO %s(unique_key, date, trend, direction, signal, last_price, candle,
                            nearest_strike, nearest_pe_oi, nearest_ce_oi, nearest_pcr, nearest_gap,
                            next_strike, next_pe_oi, next_ce_oi, next_pcr, next_gap) VALUES %%s""" % (to_data_table)

            # Convert DataFrame to tuples for bulk insert
            tuples = [tuple(x) for x in sorted_df.to_numpy()]
    
            # Insert data using a bulk insert method
            self.db_conn.insert_bulk_data(query, tuples)
    
            # Commit transaction
            self.db_conn.commit()

            return True
        except Exception as e:
            self.logging.error(f"Error in processing data for traning: {e}")
        finally:
            if self.db_conn is not None:
                self.db_conn.close()
        
        return None

    def fetch_ticks(self, date):
        data_table = 'tick_details_copy'
    
        from_dt = datetime(date.year, date.month, date.day, 9, 15)
        to_dt = datetime(date.year, date.month, date.day, 15, 30)
    
        try:
            self.db_conn.connect()
    
            query = f"""
                SELECT id, token, date, last_price, oi, volume_traded 
                FROM %s
                WHERE date >= '%s' AND date <= '%s'
                ORDER BY id
            """ % (data_table, from_dt, to_dt)
            
            df = self.db_conn.get_records_in_data_frame(query)
            return df
    
        except Exception as e:
            self.logging.error("Error in tick_details_copy data", exc_info=True)
    
        finally:
            if self.db_conn is not None:
                self.db_conn.close()
    
        return None



    
        
        