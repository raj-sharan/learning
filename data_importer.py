#!/Users/grajwade/vPython/bin/python

import sys
import logging
from datetime import datetime, timedelta

from historetical_data import HistoricalData
from settings import Setting
from kite_login import KiteLogin
from common import Util

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG,
	format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

setting = Setting()
setting.set_request_token("xkgKRjmfoy31iraruMXvRnqVRyh236Gu")

tokens = setting.securities.keys()

kite_login = KiteLogin(setting, logging)
kite_login.connect()
print(kite_login.is_connected())

if not kite_login.is_connected():
    logging.error(f"Failed to connect Kite")
    exit()

start_time = datetime.now()
start_time = datetime(start_time.year, start_time.month, start_time.day, 9, 10)
unique_key = Util.generate_5m_id(start_time)

logging.info("Cleaning old records")

h_data = HistoricalData(setting, "", logging)
if not h_data.prepare(True):
    logging.error("Cleaning old records failed")
    exit()

for token in tokens:
    h_data = HistoricalData(setting, token, logging)
    # if h_data.sync_five_min_test_data(kite_login):
    if h_data.is_data_synced(setting.table_name_5m) or h_data.sync_five_min_data(kite_login):
        logging.info(f"Synced 5min data for: {token}")
        if len(h_data.load_5min_data(unique_key)) > h_data.required_5m_data_count:
            logging.info(f"5min data ({len(h_data.data_5min)}) synced for: {token}")
        
        if h_data.is_data_synced(setting.table_name_30m) or h_data.sync_thirty_min_data(kite_login):
            logging.info(f"Synced 30min data for: {token}")
            if len(h_data.load_30min_data()) > h_data.required_30m_data_count:
                logging.info(f"30min data ({len(h_data.data_30min)}) synced for: {token}")
        else:
            logging.error(f"Failed to sync 30min data for: {token}")
    else:
        logging.error(f"Failed to sync 5min data for: {token}")
    