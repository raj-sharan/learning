import json
from datetime import datetime, timedelta

class Setting:

     # https://kite.zerodha.com/connect/login?api_key=d49h8m53givb5cme
    def __init__(self):
        self.last_loaded_at = None
        self.reload()
        
    def reload(self):
        if self.last_loaded_at is not None and datetime.now() - self.last_loaded_at <= timedelta(seconds = 15):
            return self
            
        with open('config/settings.json', 'r') as file:
            self.settings       = json.load(file)
            self.api_key        = self.settings.get("api_key")
            self.api_secret     = self.settings.get("api_secret")
            self.kite_id        = self.settings.get("kite_id")
            self.securities     = self.settings.get("securities")
            self.manage_position = self.settings.get("manage_position")
            self.db_name        = self.settings.get("db_name")
            self.db_username    = self.settings.get("db_username")
            self.db_password    = self.settings.get("db_password")
            self.db_host        = self.settings.get("db_host")
            self.db_port        = self.settings.get("db_port")
            self.table_name_5m  = self.settings.get("table_name_5m")
            self.table_name_30m = self.settings.get("table_name_30m")
            
        self.last_loaded_at = datetime.now()
        return self
    
    def as_json(self):
        return self.settings.update({"request_token": self.request_token})
    
    def set_request_token(self, request_token):
        self.request_token = request_token

    def get_security_token_by_symbol(self, symbol):
        for token, details in self.securities.items():
            if details.get("symbol") == symbol:
                return details['token']
        return None

    def get_security_by_token(self, token):
        return self.securities.get(str(token))

    def get_securities_tokens(self):
        return [value.get('token') for key, value in self.securities.items()]


    # def get_nse_instruments(self, symbol):
    #     if self.nse_instruments is None:
    #         with open('config/nse_instruments.json', 'r') as file:
    #             self.nse_instruments = json.load(file)
    #     return [item for item in self.nse_instruments if item['tradingsymbol'].startswith(symbol)]

    # def get_nse_instrument_by_token(self, token):
    #     if self.nse_instruments is None:
    #         with open('config/nse_instruments.json', 'r') as file:
    #             self.nse_instruments = json.load(file)
    #     return next((item for item in self.nse_instruments if str(item['token']) == str(token)), None)

    # def update_instruments(self, kit, file_name, exchange=None):
    #     instruments = kit.instruments(exchange=exchange)
    #     file_path = f"config/{file_name}"
    #     with open(file_path, 'w') as file:
    #         json.dump(instruments, file, indent=4)