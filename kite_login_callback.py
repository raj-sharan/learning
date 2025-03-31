#!/Users/grajwade/vPython/bin/python

import webbrowser
import logging
import sys
import time
import os
import threading

from flask import Flask, request

from settings import Setting
from kite_login import KiteLogin

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG,
	format="%(asctime)s[%(levelname)s] - %(message)s")

class KiteLoginCallback:
    def __init__(self, setting, logging):
        self.setting = setting
        self.api_key = self.setting.api_key
        self.app = Flask(__name__)
        self.logging = logging
        self.port = 5000
        self.continue_app = False
        self.setup_routes()
        
    def setup_routes(self):
        @self.app.route("/")
        def home():
            return "Kite API Login Callback Running"

        @self.app.route("/login")
        def login():
            login_url = f"https://kite.zerodha.com/connect/login?api_key={self.api_key}"
            webbrowser.open(login_url)  # Opens login page in browser
            return f"Login initiated. Please complete authentication: {login_url}"

        @self.app.route("/callback")
        def callback():
            request_token = request.args.get("request_token")  # Extract request token from URL
            if request_token:
                file_path = "config/session.txt"
                if os.path.exists(file_path):
                    os.remove(file_path)  # Correct way to delete a file
                    self.logging.info(f"File {file_path} deleted successfully.")
                self.setting.set_request_token(request_token)
                kite_login = KiteLogin(self.setting, self.logging)
                if kite_login.connect():
                    self.continue_app = False
                    return "Collected to kite"
                else:
                    return f"Faled to connect kite using request_token '{request_token}'"
    
            return "Failed to get request token and connect", 400
            

    def start_server(self):
        """Runs the Flask app in a separate thread"""
        thread = threading.Thread(target=self.app.run, kwargs={"debug": True, "port": self.port, "use_reloader": False})
        thread.daemon = True  # Allows Flask to stop when the script ends
        thread.start()

    def launch_login(self):
        """Launches the browser to start login process"""
        login_url = f"http://127.0.0.1:{self.port}/login"
        webbrowser.open(login_url)

    def start(self):
        """Starts the Flask server and launches login"""
        self.start_server()
        self.continue_app = True  
        self.launch_login()
        while self.continue_app:
            time.sleep(5)

# ✅ Example Usage
if __name__ == "__main__":
    setting = Setting()
    kite_callback = KiteLoginCallback(setting, logging)
    kite_callback.start()
    
