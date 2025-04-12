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
        
    def generate(self, current_date, end_date):
        data_table = 'processed_details'

        from_dt = datetime(current_date.year, current_date.month, current_date.day, 9, 15)
        to_dt   = datetime(end_date.year, end_date.month, end_date.day, 15, 30)
        try:
            from_unique_key = Util.generate_5m_id(from_dt)
            to_unique_key = Util.generate_5m_id(to_dt)
            
            self.db_conn.connect()
            query = """SELECT unique_key, date, direction,
                        ce_beta, ce_oi, old_ce_oi, pe_beta, pe_oi, old_pe_oi,
                        ce_curr_oi, pe_curr_oi FROM %s
            where unique_key >= %s and unique_key < %s
            """ % (data_table, from_unique_key, to_unique_key)
            
            data_df = self.db_conn.get_records_in_data_frame(query)

            data_df['time'] = data_df.apply(lambda row: Util.generate_time_id(row["date"]), axis=1)
            
            data_df['ce_pe_oi_ratio'] = data_df.apply(self.ce_pe_oi_ratio, axis=1)
            data_df['prev_ce_pe_oi_ratio'] = data_df.apply(self.prev_ce_pe_oi_ratio, axis=1)
            data_df['ce_oi_change'] = data_df.apply(self.ce_oi_change, axis=1)
            data_df['pre_ce_oi_change'] = data_df.apply(self.pre_ce_oi_change, axis=1)
        
            data_df['pe_oi_change'] = data_df.apply(self.pe_oi_change, axis=1)
            data_df['pre_pe_oi_change'] = data_df.apply(self.pre_pe_oi_change, axis=1)

            data_df['state'] = data_df.apply(self.determine_action, axis=1)
            data_df['action'] = data_df.apply(self.determine_action, axis=1)
            
            if not os.path.exists('traning_data.csv'):
                data_df.to_csv('traning_data.csv', index=False)
            else:
                self.logging.error("Please the delete file 'traning_data.csv' first, Already exists")
        except Exception as e:
            self.logging.error(f"Error in processing data for traning: {e}")
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
        
            
    def load_traning_data(self, csv):
        data_df = pd.read_csv(csv)

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

    def fetch_traning_data(self, from_date):
        data_table = 'traning_data'
        
        from_dt = datetime(from_date.year, from_date.month, from_date.day, 9, 15)
        try:
            from_unique_key = Util.generate_5m_id(from_dt)
            
            self.db_conn.connect()
            query = """SELECT unique_key, date, time, direction, ce_pe_oi_ratio, prev_ce_pe_oi_ratio,
                        ce_beta, ce_oi_change, pre_ce_oi_change,
                        pe_beta, pe_oi_change, pre_pe_oi_change, state, action FROM %s
            where unique_key >= %s order by date
            """ % (data_table, from_unique_key)
            
            df = self.db_conn.get_records_in_data_frame(query)

            return df
        except Exception as e:
            self.logging.error(f"Error in processing data for traning: {e}")
        finally:
            if self.db_conn is not None:
                self.db_conn.close()
        
        return None


    
        
        