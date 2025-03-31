import os

from datetime import datetime
from kiteconnect import KiteConnect, exceptions

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
        file_path = "config/session.txt"
        try:
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

            if self.is_connected():
                self.logging.info(f"Connected to kite successfully: {self.conn}")
                return self.conn
            else:
                self.logging.error(f"Unable to connected to kite")
                      
        except exceptions.TokenException as e:
            if os.path.exists(file_path):
                os.remove(file_path)  # Correct way to delete a file
                print(f"File {file_path} deleted successfully.")
        except Exception as e:
            self.logging.error(e.__class__)
            self.logging.error(f"Error in connecting kite connect: {e}")
        
        return None

        
    def is_connected(self):
        profile = self.conn.profile()
        if profile["user_id"] == self.setting.kite_id:
            self.profile_name = profile["user_name"]
            self.logging.info(f"User Profile: == * {self.profile_name} * ==")
            return True

        return False
