import os

from datetime import datetime
from kiteconnect import KiteConnect

class KiteLogin:
    def __init__(self, setting, logging):
        self.conn = None
        self.setting = setting
        self.profile_name = None
        self.access_token = None
        self.logging = logging

    def request_token_url(self):
        return self.conn.login_url()
        
    def connect(self):
        try:
            file_path = "config/session.txt"
            
            self.conn = KiteConnect(api_key = self.setting.api_key)
            if os.path.exists(file_path):
                with open(file_path, 'r') as file:
                    access_token = file.read()
            else:
                session = self.conn.generate_session(
                self.setting.request_token, api_secret = self.setting.api_secret
                )
                access_token = session["access_token"]
                with open(file_path, 'w') as file:
                    file.write(session["access_token"])
            
            self.access_token = access_token
            self.conn.set_access_token(access_token)
           
        except Exception as e:
            self.logging.error(f"Error in connecting kite connect: {e}")
            return None

        if not self.is_connected():
            return None

        self.logging.info(f"Connected to kite successfully: {self.conn}")
        
        return self.conn
        
    def is_connected(self):
        try:
            profile = self.conn.profile()
            if profile["user_id"] == self.setting.kite_id:
                self.profile_name = profile["user_name"]
                self.logging.info(f"User Profile: == * {self.profile_name} * ==")
                return True
        except Exception as e:
            self.logging.error(f"Error checking connection: {e}")
            return False

        return False
