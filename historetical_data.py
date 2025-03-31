from datetime import datetime, timedelta
import pandas as pd
import time
import traceback

from db_connect import PostgresDB
from common import Util

class HistoricalData:

    def __init__(self, setting, token, logging):
        self.token = token
        self.setting = setting
        self.db_conn = PostgresDB(setting, logging)
        self.required_5m_data_count = 210
        self.required_30m_data_count = 15
        self.data_5min = None
        self.data_30min = None
        self.logging = logging

    def prepare(self, force = False):
        if force or not self.any_five_min_data_synced():
            if not self.clean_pre_records(self.setting.table_name_5m):
                return False

        if force or not self.any_thirty_min_data_synced():
            if not self.clean_pre_records(self.setting.table_name_30m):
                return False
          
        return True


    def any_five_min_data_synced(self):
        fetched = False
        try:
            self.db_conn.connect()
            today = datetime.now().strftime('%Y-%m-%d')
            query = f"SELECT COUNT(*) FROM {self.setting.table_name_5m} WHERE created_at >= '{today} 00:00:00'"
            result = self.db_conn.fetch_records(query)
            fetched = result[0][0] > 0
        except Exception as e:
            self.logging.error(f"Error fetching 5m data for {self.token}: {e}")
        finally:
            self.db_conn.close()
        return fetched

    def any_thirty_min_data_synced(self):
        fetched = False
        try:
            self.db_conn.connect()
            today = datetime.now().strftime('%Y-%m-%d')
            query = f"SELECT COUNT(*) FROM {self.setting.table_name_30m} WHERE created_at >= '{today} 00:00:00'"
            result = self.db_conn.fetch_records(query)
            fetched = result[0][0] > 0
        except Exception as e:
            self.logging.error(f"Error fetching 30m data for {self.token}: {e}")
        finally:
            self.db_conn.close()
        return fetched

    def is_data_synced(self, table_name):
        synced = False
        try:
            self.db_conn.connect()
            today = datetime.now().strftime('%Y-%m-%d')
            query = f"SELECT COUNT(*) FROM {table_name} WHERE token = {self.token} and created_at >= '{today} 00:00:00'"
            result = self.db_conn.fetch_records(query)
            synced = result[0][0] > 0
        except Exception as e:
            self.logging.error(f"Error fetching 30m data for {self.token}: {e}")
        finally:
            self.db_conn.close()
        return synced

    def sync_five_min_data(self, kite_login):
        collected    = False
        current_time = datetime.now()
        started_at   = datetime.now()
    
        self.logging.info(f"Started fetching {self.token} at {started_at}")
        columns   = ['date', 'open', 'high', 'low', 'close', 'volume']
        full_data = pd.DataFrame(columns=columns)
        count = 0
        while not collected:
            count = count + 1
            self.logging.info(f"looping - {count}")
            if datetime.now() - started_at > timedelta(minutes = 5):
                self.logging.error(f"Taking too much time in fetching {self.token}, ending at {datetime.now()}")
                return False
    
            start_day = current_time - timedelta(days = 3)
            end_day   = current_time - timedelta(days = 1)

            from_dt = datetime(start_day.year, start_day.month, start_day.day, 9, 0)
            to_dt   = datetime(end_day.year, end_day.month, end_day.day, 16, 0)

            self.logging.info(f"from_dt {from_dt} to_dt {to_dt}")
            try:
                data = kite_login.conn.historical_data(self.token, from_dt, to_dt, "5minute")
            except Exception as e:
                self.logging.error(f"Error in sync 5m pre-data for {self.token}: {e}")
                return False
    
            data_df   = pd.DataFrame(data)
            full_data = data_df.copy() if full_data.empty else pd.concat([full_data, data_df], ignore_index=True)
    
            if len(full_data) >= self.required_5m_data_count:
                collected = True
            else:
                current_time = start_day - timedelta(days = 1)
            time.sleep(5)
    
        self.logging.info(f"Ended fetching {len(full_data)} data points for {self.token} at {datetime.now()}")

        full_data = full_data.drop(columns = ["volume"])
        full_data['date'] = full_data.apply(lambda row: Util.parse_datetime(row["date"]), axis=1)
        full_data         = full_data.sort_values(by = 'date', ascending = True)

        full_data["unique_key"] = full_data.apply(lambda row: Util.generate_5m_id(row["date"]), axis=1)
        full_data["token"]      = full_data.apply(lambda row: self.token, axis=1)
        
        #full_data.to_csv('NIFTY50.csv')
        
        return self.save_data_to_db(full_data, self.setting.table_name_5m)

    def sync_five_min_data_for_day(self, kite_login, current_time):
        collected    = False
        started_at   = datetime.now()
    
        self.logging.info(f"Started fetching {self.token} at {started_at}")
        columns   = ['date', 'open', 'high', 'low', 'close', 'volume']
        full_data = pd.DataFrame(columns=columns)
        count = 0
        if True:
            count = count + 1
            self.logging.info(f"looping - {count}")
            if datetime.now() - started_at > timedelta(minutes = 5):
                self.logging.error(f"Taking too much time in fetching {self.token}, ending at {datetime.now()}")
                return False
    
            start_day = current_time
            end_day   = current_time

            from_dt = datetime(start_day.year, start_day.month, start_day.day, 9, 0)
            to_dt   = datetime(end_day.year, end_day.month, end_day.day, 16, 0)

            self.logging.info(f"from_dt {from_dt} to_dt {to_dt}")
            try:
                data = kite_login.conn.historical_data(self.token, from_dt, to_dt, "5minute")
            except Exception as e:
                self.logging.error(f"Error in sync 5m pre-data for {self.token}: {e}")
                return False
    
            data_df   = pd.DataFrame(data)
            full_data = data_df.copy() if full_data.empty else pd.concat([full_data, data_df], ignore_index=True)
    
    
        self.logging.info(f"Ended fetching {len(full_data)} data points for {self.token} at {datetime.now()}")

        full_data = full_data.drop(columns = ["volume"])
        full_data['date'] = full_data.apply(lambda row: Util.parse_datetime(row["date"]), axis=1)
        full_data         = full_data.sort_values(by = 'date', ascending = True)

        full_data["unique_key"] = full_data.apply(lambda row: Util.generate_5m_id(row["date"]), axis=1)
        full_data["token"]      = full_data.apply(lambda row: self.token, axis=1)
        
        #full_data.to_csv('NIFTY50.csv')
        
        return self.save_data_to_db(full_data, self.setting.table_name_5m)
        
    def sync_five_min_test_data(self, kite_login):
        collected    = False
        current_time = datetime.now()
        started_at   = datetime.now()
    
        self.logging.info(f"Started fetching {self.token} at {started_at}")
        columns   = ['date', 'open', 'high', 'low', 'close', 'volume']
        full_data = pd.DataFrame(columns=columns)
        count = 0
        while not collected:
            count = count + 1
            self.logging.info(f"looping - {count}")
            if datetime.now() - started_at > timedelta(minutes = 30):
                self.logging.error(f"Taking too much time in fetching {self.token}, ending at {datetime.now()}")
                return False
    
            start_day = current_time - timedelta(days = 20)
            end_day   = current_time - timedelta(days = 1)

            from_dt = datetime(start_day.year, start_day.month, start_day.day, 9, 0)
            to_dt   = datetime(end_day.year, end_day.month, end_day.day, 16, 0)

            self.logging.info(f"from_dt {from_dt} to_dt {to_dt}")
            try:
                data = kite_login.conn.historical_data(self.token, from_dt, to_dt, "5minute")
            except Exception as e:
                self.logging.error(f"Error in sync 5m pre-data for {self.token}: {e}")
                return False
    
            data_df   = pd.DataFrame(data)
            full_data = data_df.copy() if full_data.empty else pd.concat([full_data, data_df], ignore_index=True)
    
            if len(full_data) >= self.required_5m_data_count:
                collected = True
            else:
                current_time = start_day
            time.sleep(5)
    
        self.logging.info(f"Ended fetching {len(full_data)} data points for {self.token} at {datetime.now()}")

        full_data = full_data.drop(columns = ["volume"])
        full_data['date'] = full_data.apply(lambda row: Util.parse_datetime(row["date"]), axis=1)
        full_data         = full_data.sort_values(by = 'date', ascending = True)

        full_data["unique_key"] = full_data.apply(lambda row: Util.generate_5m_id(row["date"]), axis=1)
        full_data["token"]      = full_data.apply(lambda row: self.token, axis=1)
        
        #full_data.to_csv('NIFTY50.csv')
        
        return self.save_data_to_db(full_data, self.setting.table_name_5m)

    def save_data_to_db(self, df, table_name):
        saved      = False
        batch_size = 500
        
        try:
            query = "INSERT INTO %s(date, open, high, low, close, unique_key, token) VALUES %%s" % (table_name)
            self.db_conn.connect()
            
            for i in range(0, len(df), batch_size):
                batch = df.iloc[i:i + batch_size]
                tuples = [tuple(x) for x in batch.to_numpy()] 
                
                self.db_conn.insert_bulk_data(query, tuples)
                
            self.db_conn.commit()
            saved = True
        except Exception as e:
            self.logging.error(f"Error in saving pre data for {self.token} in #{table_name}: {e}")
            self.db_conn.rollback()
        finally:
            if self.db_conn is not None:
                self.db_conn.close()
                
        return saved
        
        
    def sync_thirty_min_data(self, kite_login):
        collected    = False
        current_time = datetime.now()
        started_at   = datetime.now()
    
        self.logging.info(f"Started fetching {self.token} at {started_at}")
        columns   = ['date', 'open', 'high', 'low', 'close', 'volume']
        full_data = pd.DataFrame(columns=columns)
        count = 0
        while not collected:
            count = count + 1
            self.logging.info(f"looping - {count}")
            if datetime.now() - started_at > timedelta(minutes = 5):
                self.logging.error(f"Taking too much time in fetching {self.token} 30m data, ending at {datetime.now()}")
                return False
    
            start_day = current_time - timedelta(days = 2)
            end_day   = current_time - timedelta(days = 1)

            from_dt = datetime(start_day.year, start_day.month, start_day.day, 9, 0)
            to_dt   = datetime(end_day.year, end_day.month, end_day.day, 16, 0)

            self.logging.info(f"from_dt {from_dt} to_dt {to_dt}")
            try:
                data = kite_login.conn.historical_data(self.token, from_dt, to_dt, "30minute")
            except Exception as e:
                self.logging.error(f"Error in sync 5m pre-data for {self.token}: {e}")
                return False
    
            data_df   = pd.DataFrame(data)
            full_data = data_df.copy() if full_data.empty else pd.concat([full_data, data_df], ignore_index=True)
    
            if len(full_data) >= self.required_30m_data_count:
                collected = True
            else:
                current_time = start_day
            time.sleep(5)
    
        self.logging.info(f"Ended fetching {len(full_data)} data points for {self.token} 30min data at {datetime.now()}")

        full_data         = full_data.drop(columns = ["volume"])
        full_data['date'] = full_data.apply(lambda row: Util.parse_datetime(row["date"]), axis=1)
        full_data         = full_data.sort_values(by = 'date', ascending = True)

        full_data["unique_key"] = full_data.apply(lambda row: Util.generate_30m_id(row["date"]), axis=1)
        full_data["token"]      = full_data.apply(lambda row: self.token, axis=1)
        
        return self.save_data_to_db(full_data, self.setting.table_name_30m)

    def load_5min_data(self, unique_key):
        try:
            if self.data_5min is None:
                self.db_conn.connect()
                query = """SELECT date, open, high, low, close, unique_key, token FROM %s
                where token = %s and unique_key < %s
                """ % (self.setting.table_name_5m, self.token, unique_key) 
                
                self.data_5min = self.db_conn.get_records_in_data_frame(query)
        except Exception as e:
            self.logging.error(f"Error in loading 5min data for {self.token}: {e}")
        finally:
            if self.db_conn is not None:
                self.db_conn.close()

        return self.data_5min

    def load_5m_current_data(self, from_dt, to_dt):
        try:
            from_unique_key = Util.generate_5m_id(from_dt)
            to_unique_key = Util.generate_5m_id(to_dt)
            
            self.db_conn.connect()
            query = """SELECT date, open, high, low, close, token FROM %s
            where token = %s and unique_key >= %s and unique_key < %s
            """ % (self.setting.table_name_5m, self.token, from_unique_key, to_unique_key)
            
            return self.db_conn.get_records_in_data_frame(query)
               
        except Exception as e:
            self.logging.error(f"Error in loading 5min data for {self.token}: {e}")
        finally:
            if self.db_conn is not None:
                self.db_conn.close()

        return pd.DataFrame([])
        
    def load_30min_data(self):
        try:
            if self.data_30min is None:
                self.db_conn.connect()
                query = "SELECT date, open, high, low, close, unique_key, token FROM %s where token = %s" % (self.setting.table_name_30m, self.token) 
                
                self.data_30min = self.db_conn.get_records_in_data_frame(query)
        except Exception as e:
            self.logging.error(f"Error in loading 30min data for {self.token}: {e}")
        finally:
            if self.db_conn is not None:
                self.db_conn.close()

        return self.data_30min
        
    def clean_pre_records(self, table_name):
        executed = False
        try:
            self.db_conn.connect()
            query = f"TRUNCATE {table_name} RESTART IDENTITY"
            self.db_conn.execute_query(query)
            query2 = f"TRUNCATE tick_details RESTART IDENTITY"
            self.db_conn.execute_query(query2)
            self.db_conn.commit()
            executed = True
        except Exception as e:
            self.logging.error(f"Error cleaning records for {self.token}: {e}")
            self.logging.error(traceback.format_exc())
            
        finally:
            self.db_conn.close()
        return executed
        