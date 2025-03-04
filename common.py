import re

from datetime import datetime, timedelta
import pandas as pd

class Util:
    
    @staticmethod
    def generate_id(time):
        if isinstance(time, str):
            time = pd.to_datetime(time)
            
        return (time.year * 100000000 +
                time.month * 1000000 +
                time.day * 10000 +
                time.hour * 100 +
                time.minute)

    @staticmethod
    def generate_5m_id(time):
        if isinstance(time, str):
            time = pd.to_datetime(time)
            
        return (time.year * 100000000 +
                time.month * 1000000 +
                time.day * 10000 +
                time.hour * 100 +
                (time.minute // 5) * 5) 

    @staticmethod
    def generate_30m_id(time):
        if isinstance(time, str):
            time = pd.to_datetime(time)
            
        from_dt = datetime(time.year, time.month, time.day, 9, 15)
        minutes_since_from_dt = (time - from_dt).total_seconds() // 60
        rounded_minutes = (minutes_since_from_dt // 30) * 30
    
        to_dt = from_dt + timedelta(minutes=rounded_minutes)
        return (to_dt.year * 100000000 +
                to_dt.month * 1000000 +
                to_dt.day * 10000 +
                to_dt.hour * 100 +
                to_dt.minute)

    @staticmethod
    def is_index_token(setting, token):
        return setting.get_security_by_token(token)['index']

    @staticmethod
    def get_parent_token(symbol):
        today = datetime.now()
        year = today.strftime("%y")
       
        return securities[token]['index']

    @staticmethod
    def parse_datetime(time):
        if isinstance(time, str):
            time = pd.to_datetime(time)
            
        return datetime(time.year, time.month, time.day, time.hour, time.minute)
        
        

# # Get current time
# current_time = datetime.now().astimezone()

# # Generate ID
# unique_id = Util.generate_id(current_time)
# print(unique_id)