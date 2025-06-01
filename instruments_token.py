import re
from datetime import datetime, timedelta

class InstrumentToken:  # Fixed typo from "IntrumentToken"

    def __init__(self, setting, logging):
        self.instrument_tokens = None
        self.setting = setting
        self.logging = logging
        self.selected_tokens = None

    def load_instrument_tokens(self, kite_login):
        if self.instrument_tokens is None:
            try:
                self.instrument_tokens = kite_login.conn.instruments(exchange='NFO')
            except Exception as e:
                self.logging.error(f"Failed to load instrument_tokens: {e}")  # Use print instead of self.logging
  
    def strike_price_tokens(self, token, current_data, additional_strikes = []):
        if not self.instrument_tokens or not current_data or current_data.get('price') is None:
            return None

        token_info = self.setting.get_security_by_token(token)
        if not token_info:
            return None
    
        strike_step = token_info['price_step']
        symbol = token_info['symbol']
        nearest_strike = int((current_data['price'] // strike_step) * strike_step)
    
        strikes = [nearest_strike + i * strike_step for i in range(-3, 4)]  # Generates 4 strikes: -1, 0, 1, 2
        if additional_strikes:
            strikes += additional_strikes
            strikes = list(set(strikes))  # Remove duplicates if needed
    
        # Check if strikes match previously selected tokens
        if self.selected_tokens and set(strikes) == set(self.selected_tokens.get('strikes', [])):
            return self.selected_tokens
            
        strikes_pattern = "|".join(map(str, strikes))

        today = datetime.now()
        year = today.strftime("%y")
        month_int = str(int(today.strftime("%m")))
        month_str = today.strftime("%b").upper()
        
        # Generate day range for current month
        days_str = "|".join(f"{(today + timedelta(days=i)).day:02d}" for i in range(7)) or "XXXXXXX"
        
        # Create regex patterns
        re_ex_days = rf"^{symbol}{year}{month_int}({days_str})({strikes_pattern})(CE|PE)$"
        re_ex_month = rf"^{symbol}{year}{month_str}({strikes_pattern})(CE|PE)$"
    
        # Check which pattern has matches
        has_days = any(re.match(re_ex_days, item["tradingsymbol"]) for item in self.instrument_tokens)
        has_month = any(re.match(re_ex_month, item["tradingsymbol"]) for item in self.instrument_tokens)
    
        # Adjust pattern if no matches found
        if not (has_days or has_month):
            next_day = today + timedelta(days=7)
            if next_day.month > today.month or next_day.year > today.year:
                start_day = datetime(next_day.year, next_day.month, 1)
                day_range = [f"{day:02d}" for day in range(start_day.day, next_day.day + 1)]
                days_str = "|".join(day_range) or "XXXXXXX"
                year = start_day.strftime("%y")
                month_int = str(int(start_day.strftime("%m")))
                re_ex = rf"^{symbol}{year}{month_int}({days_str})({strikes_pattern})(CE|PE)$"
            else:
                re_ex = re_ex_month  # Fallback
        else:
            re_ex = re_ex_days if has_days else re_ex_month
    
        # Initialize token storage
        self.selected_tokens = {
            'token_list': [],
            'nearest_pre_strike': nearest_strike - strike_step, 
            'nearest_strike': nearest_strike, 
            'next_strike': nearest_strike + strike_step, 
            'next_pre_strike': nearest_strike + strike_step * 2,
            'strikes': [],
            'ce_tokens': {},
            'pe_tokens': {},
            'curr_ce_token': None,
            'curr_pe_token': None
        }
        for strike in strikes:
            self.selected_tokens[strike] = {'CE': None, 'PE': None}
    
        # Match and assign tokens
        for item in self.instrument_tokens:
            if re.match(re_ex, item["tradingsymbol"]):
                strike = int(item['strike'])
                self.selected_tokens['strikes'].append(strike)
                option_type = 'CE' if item["tradingsymbol"].endswith('CE') else 'PE'
                self.selected_tokens[strike][option_type] = item
                self.selected_tokens['token_list'].append(item["instrument_token"])
                if option_type == 'CE':
                    self.selected_tokens['ce_tokens'][item["instrument_token"]] = item
                elif option_type == 'PE':
                    self.selected_tokens['pe_tokens'][item["instrument_token"]] = item
                if strike == nearest_strike and option_type == 'CE':
                    self.selected_tokens['curr_ce_token'] = item["instrument_token"]
                elif strike == nearest_strike + strike_step and option_type == 'PE':
                    self.selected_tokens['curr_pe_token'] = item["instrument_token"]
                
        return self.selected_tokens

    def get_token_by_symbol(self, symbol):
        tokens = [item["instrument_token"] for item in self.instrument_tokens if re.match(symbol, item["tradingsymbol"])]
        return tokens[0] if tokens else None  # Return first token or None if not found

    def get_symbol_by_token(self, token):
        tokens = [item["tradingsymbol"] for item in self.instrument_tokens if token == item["instrument_token"]]
        return tokens[0] if tokens else None  # Return first token or None if not found

    def get_name_by_token(self, token):
        tokens = [item["name"] for item in self.instrument_tokens if token == item["instrument_token"]]
        return tokens[0] if tokens else None  # Return first token or None if not found

    def get_quantity(self, token):
        token_info = self.setting.get_security_by_token(token)
        if not token_info:
            return None

        return token_info['quantity']

    def get_symbols_from_tokens(self, tokens):
        return [f"{item["instrument_token"]}:{item["tradingsymbol"]}" for item in self.instrument_tokens if item["instrument_token"] in tokens]

    def get_strike_price(self, tokens):
        return [int(item["strike"]) for item in self.instrument_tokens if item["instrument_token"] in tokens]
