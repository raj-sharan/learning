import re
from datetime import datetime, timedelta

class InstrumentToken:  # Fixed typo from "IntrumentToken"

    def __init__(self, setting, logging):
        self.instrument_tokens = None
        self.setting = setting
        self.logging = logging

    def load_instrument_tokens(self, kite_login):
        if self.instrument_tokens is None:
            try:
                self.instrument_tokens = kite_login.conn.instruments(exchange='NFO')
            except Exception as e:
                self.logging.error(f"Failed to load instrument_tokens: {e}")  # Use print instead of self.logging
  
    def strike_price_tokens(self, token, current_data):  # Added `self`
        if self.instrument_tokens is None or current_data is None or current_data['price'] is None:
            return None

        today = datetime.now()
        
        # Generate a list of days from today to next_day (6 days ahead)
        next_day = today + timedelta(days=6)
        day_range = [f"{day:02d}" for day in range(today.day, next_day.day + 1)]

        # Join the days with "|"
        days_str = "|".join(day_range)

        if not days_str:
            days_str = "XXXXXXX"
        
        year = today.strftime("%y")  # Two-digit year (25 for 2025)
        month_int = str(int(today.strftime("%m")))
        month_str = today.strftime("%b").upper()  # Abbreviated month (e.g., FEB)

        token_info = self.setting.get_security_by_token(token)
        if not token_info:
            return None  # Return None if token info is not found

        strike_step = token_info['price_step']
        symbol = token_info['symbol']
        
        # Round price to nearest strike price
        nearest_strike = int((current_data['price'] // strike_step) * strike_step)

        # Define call (CE) and put (PE) strike price ranges
        ce_strikes = [nearest_strike]  
        pe_strikes = [nearest_strike + 100]  

        # Convert lists to regex-friendly strings
        ce_strikes_pattern = "|".join(map(str, ce_strikes))  
        pe_strikes_pattern = "|".join(map(str, pe_strikes))  

        # Construct regex pattern
        ce_re_ex_days  = rf"^{symbol}{year}{month_int}({days_str})({ce_strikes_pattern})CE$"
        ce_re_ex_month = rf"^{symbol}{year}{month_str}({ce_strikes_pattern})CE$"
        
        # Check if we have matching call option symbols
        has_ce_matches_with_days = any(re.match(ce_re_ex_days, item["tradingsymbol"]) for item in self.instrument_tokens)
        has_ce_matches_with_month = any(re.match(ce_re_ex_month, item["tradingsymbol"]) for item in self.instrument_tokens)

        if not (has_ce_matches_with_days or has_ce_matches_with_month):
            if next_day.month > today.month or next_day.year > today.year:
                start_day = datetime(next_day.year, next_day.month, 1, 00, 00)
                day_range = [f"{day:02d}" for day in range(start_day.day, next_day.day + 1)]
    
                # Join the days with "|"
                days_str = "|".join(day_range)
    
                if not days_str:
                    days_str = "XXXXXXX"
                    
                year = start_day.strftime("%y")  # Two-digit year (25 for 2025)
                month_int = str(int(start_day.strftime("%m")))

            ce_re_ex = rf"^{symbol}{year}{month_int}({days_str})({ce_strikes_pattern})CE$"
            pe_re_ex = rf"^{symbol}{year}{month_int}({days_str})({pe_strikes_pattern})PE$"
        elif not has_ce_matches_with_days:
            ce_re_ex = rf"^{symbol}{year}{month_str}({ce_strikes_pattern})CE$"
            pe_re_ex = rf"^{symbol}{year}{month_str}({pe_strikes_pattern})PE$"
        else:
            ce_re_ex = rf"^{symbol}{year}{month_int}({days_str})({ce_strikes_pattern})CE$"
            pe_re_ex = rf"^{symbol}{year}{month_int}({days_str})({pe_strikes_pattern})PE$"

        ce_tokens = [item["tradingsymbol"] for item in self.instrument_tokens if re.match(ce_re_ex, item["tradingsymbol"])]
        pe_tokens = [item["tradingsymbol"] for item in self.instrument_tokens if re.match(pe_re_ex, item["tradingsymbol"])]
        
        return {"ce_tokens": ce_tokens, "pe_tokens": pe_tokens}

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
