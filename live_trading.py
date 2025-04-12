#!/Users/grajwade/vPython/bin/python

import re
import sys
import json
import time
import logging
import traceback
import warnings
from datetime import datetime, timedelta
from kiteconnect import KiteConnect, KiteTicker

from settings import Setting
from kite_login import KiteLogin
from live_data import LiveData
from common import Util
from instrument import Instrument
from instruments_token import InstrumentToken
from order_handler import OrderHandler

logging.basicConfig(stream=sys.stdout, level=logging.INFO,
	format="%(asctime)s[%(levelname)s] - %(message)s")

warnings.filterwarnings("ignore")

# Initialize
setting = Setting()
setting.set_request_token("rC81Z2zpaA14ew71mY4dm8e3BrqSFCux")

kite_login = KiteLogin(setting, logging)
kite_login.connect()

if kite_login.conn is None:
    logging.error("Unable to make kite connection")
    exit()

live_data = LiveData(setting, logging)

# Initialise
# connect_timeout= 60*10 (10 minutes)
# https://github.com/zerodha/pykiteconnect/blob/master/kiteconnect/ticker.py
#kws = KiteTicker(setting.api_key, kite_login.access_token, debug=True, connect_timeout = 60*40)
kws = KiteTicker(setting.api_key, kite_login.access_token, connect_timeout = 60*40)
            
def on_ticks(ws, ticks):
    # Callback to receive ticks.
    # print("Ticks: {}".format(ticks))
    live_data.collect_instruments_data(ticks)
    
def on_connect(ws, response):
    logging.info("on_connect: ".format(response))
    # Callback on successful connect.
    # Subscribe to a list of instrument_tokens (RELIANCE and ACC here).
    # ws.subscribe([738561, 256265])

    # Set RELIANCE to tick in `full` mode.
    # ws.set_mode(ws.MODE_LTP, [738561, 256265])
    
def on_order_update(ws, data):
    live_data.order_updated = True
    logging.info("on_order_update: {}".format(data))
    
def on_close(ws, code, reason):
    logging.info(f"on_close: {code}, {reason}")
    # time.sleep(5)  # Wait before reconnecting
    # logging.info("Reconnecting...")
    # connect_ws()  # Restart WebSocket connection

def on_error(ws, code, reason):
    logging.error("on_error: {} - {}".format(code, reason))

# Callback when reconnect is in progress
def on_reconnect(ws, attempts_count):
    logging.info("on_reconnect: {}".format(attempts_count))


# Assign the callbacks.
kws.on_ticks = on_ticks
kws.on_connect = on_connect
kws.on_order_update = on_order_update
kws.on_close = on_close
kws.on_error = on_error
kws.on_reconnect = on_reconnect


# Infinite loop on the main thread. Nothing after this will run.
# You have to use the pre-defined callbacks to manage subscriptions.
kws.connect(threaded=True)

            
tokens = setting.get_securities_tokens()

instrument_token = InstrumentToken(setting, logging)
instrument_token.load_instrument_tokens(kite_login)

if instrument_token.instrument_tokens is None:
    logging.error("Unable to load instrument_token")
    exit()

instruments = {}
subscribe_tokens = []
# Reload historical data
def reload_data(token, unique_key):
    if token not in instruments:
        is_index = Util.is_index_token(setting, token)
        instruments[token] = Instrument(token, is_index, setting, logging)
        instruments[token].load_historical_data(unique_key)

def subscribe_strike_price_tokens(token):
    global subscribe_tokens
    subscribe_tokens.clear()

    if token in instruments.keys():
        for order_token in instruments[token].order_ids():
            subscribe_tokens.append(order_token)
        if instruments[token].order_ids() or instruments[token].reuse_tokens:
            if instruments[token].ce_pe_token['ce_token']:
                subscribe_tokens.append(instruments[token].ce_pe_token['ce_token'])
            if instruments[token].ce_pe_token['pe_token']:
                subscribe_tokens.append(instruments[token].ce_pe_token['pe_token'])

    price_tokens = instrument_token.strike_price_tokens(token, live_data.get_current_data(token))
    if price_tokens is None:
        return []
    
    ce_tokens = price_tokens.get('ce_tokens', [])
    pe_tokens = price_tokens.get('pe_tokens', [])
    
    ce_tokens = [
        token for item in ce_tokens
        if (token := instrument_token.get_token_by_symbol(item)) is not None
    ]
    
    pe_tokens = [
        token for item in pe_tokens
        if (token := instrument_token.get_token_by_symbol(item)) is not None
    ]
    
    subscribe_tokens = list(set(ce_tokens + pe_tokens + subscribe_tokens))

    if subscribe_tokens:
        logging.info(f"subscribe_tokens: {instrument_token.get_symbols_from_tokens(subscribe_tokens)}")
        kws.subscribe(subscribe_tokens)
        kws.set_mode(kws.MODE_FULL, subscribe_tokens)
    return subscribe_tokens
       

def trading_windows(current_time):
    from_dt = datetime(current_time.year, current_time.month, current_time.day, 6, 0)
    to_dt = datetime(current_time.year, current_time.month, current_time.day, 15, 31)
    return from_dt < current_time < to_dt

start_time = datetime.now()
start_time = datetime(start_time.year, start_time.month, start_time.day, 9, 15)
unique_key = Util.generate_5m_id(start_time)

# Reload data for all tokens
for token in tokens:
    reload_data(token, unique_key)

order_handler = OrderHandler(kite_login, setting, instrument_token, logging)
try: 
    logging.info("load_positions")
    order_handler.load_positions(instruments)
except Exception as e:
    logging.error(f"Error in loading order_handler: {e}")
    logging.error(traceback.format_exc())
    exit()

# Main loop
subscribed_list = []

while True:
    current_time = datetime.now()
    
    if not trading_windows(current_time):
        logging.info("Main thread: Trading session ended")
        exit()

    from_dt = datetime(current_time.year, current_time.month, current_time.day, 9, 15)
    to_dt = datetime(current_time.year, current_time.month, current_time.day, 15, 31)

    if not (to_dt > current_time > from_dt):
        logging.info("Main thread: Waiting for Trading session start")
        time.sleep(30)
        continue
        
    subscribed_list.clear()

    print("============================================ * In Trading session * =================================================")
    
    live_data.to_s()

    if kws.is_connected():
        try:
            tokens = setting.reload().get_securities_tokens()

            kws.subscribe(tokens)
            kws.set_mode(kws.MODE_FULL, tokens)
            subscribed_list.extend(tokens)
            
            for token in tokens:
                reload_data(token, unique_key)
                
                subscribed_strike_price_tokens = subscribe_strike_price_tokens(token)
                if subscribed_strike_price_tokens:
                    subscribed_list.extend(subscribed_strike_price_tokens)

            positions_reloaded = order_handler.reload_positions(instruments)
            if positions_reloaded is not None:
                if not setting.manage_position:
                    logging.warn('Note: Manage position is disabled.')
                if positions_reloaded and setting.manage_position:
                    if order_handler.fill_orders(instruments) is True:
                        order_handler.manage_orders(instruments, live_data)

                for token in instruments.keys():
                    instruments[token].refresh_data(kite_login, current_time)
                    instruments[token].load_momentum_analysis(kite_login, live_data, instrument_token, current_time)
                    instruments[token].load_current_data_analysis(live_data, instrument_token)
                    instruments[token].print_analysis_details()
                    if not instruments[token].order_ids():
                        instruments[token].execute_trade_opportunity(kite_login, live_data, instrument_token, current_time)

            live_data.analyser.load_current_data(current_time)
            order_handler.cancel_invalid_sl_orders(live_data, instruments)
            
            # Unsubscribe unused tokens
            unused_tokens = set(kws.subscribed_tokens.keys()) - set(subscribed_list)
            # kws.unsubscribe(list(unused_tokens))
        except Exception as e:
            logging.error(f"Error on loop: {e}")
            if re.match('Error in connecting kite connect', str(e)):
                kite_login.connect()
            logging.error(traceback.format_exc())
    else:
        kws.close()
        kws.connect(threaded=True)
    
    time.sleep(5)  # Main loop delay 10 sec
