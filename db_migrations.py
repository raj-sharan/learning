#!/Users/grajwade/vPython/bin/python
import logging
import sys

from settings import Setting
from db_connect import PostgresDB

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG,
	format="%(asctime)s[%(levelname)s] - %(message)s")

historical_test_data_5m_sql = """
    CREATE TABLE IF NOT EXISTS historical_data_5m_test (
        id SERIAL PRIMARY KEY,
        token BIGINT NOT NULL,
        date TIMESTAMP NOT NULL,
        unique_key BIGINT NOT NULL,
        open DECIMAL(10, 2) NOT NULL,
        high DECIMAL(10, 2) NOT NULL,
        low DECIMAL(10, 2) NOT NULL,
        close DECIMAL(10, 2) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (token, unique_key)
    );

    -- Add an index on (token, unique_key)
    CREATE UNIQUE INDEX historical_test_data_5m_token_unique_key
    ON historical_data_5m_test (token, unique_key);
    
    -- Add an index on (token, date) for faster date-based queries
    CREATE INDEX historical_test_data_5m_token_date 
    ON historical_data_5m_test (token, date);
"""

historical_data_5m_sql = """
    CREATE TABLE IF NOT EXISTS historical_data_5m (
        id SERIAL PRIMARY KEY,
        token BIGINT NOT NULL,
        date TIMESTAMP NOT NULL,
        unique_key BIGINT NOT NULL,
        open DECIMAL(10, 2) NOT NULL,
        high DECIMAL(10, 2) NOT NULL,
        low DECIMAL(10, 2) NOT NULL,
        close DECIMAL(10, 2) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (token, unique_key)
    );

    -- Add an index on (token, unique_key)
    CREATE UNIQUE INDEX historical_data_5m_token_unique_key
    ON historical_data_5m (token, unique_key);
    
    -- Add an index on (token, date) for faster date-based queries
    CREATE INDEX historical_data_5m_token_date 
    ON historical_data_5m (token, date);
"""

historical_data_30m_sql = """
    CREATE TABLE IF NOT EXISTS historical_data_30m (
        id SERIAL PRIMARY KEY,
        token BIGINT NOT NULL,
        date TIMESTAMP NOT NULL,
        unique_key BIGINT NOT NULL,
        open DECIMAL(10, 2) NOT NULL,
        high DECIMAL(10, 2) NOT NULL,
        low DECIMAL(10, 2) NOT NULL,
        close DECIMAL(10, 2) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (token, unique_key)
    );

    -- Add an index on (token, unique_key)
    CREATE UNIQUE INDEX historical_data_30m_token_unique_key 
    ON historical_data_30m (token, unique_key);
    
    -- Add an index on (token, date) for faster date-based queries
    CREATE INDEX historical_data_30m_token_date 
    ON historical_data_30m (token, date);
"""

processed_details = """
    CREATE TABLE IF NOT EXISTS processed_details (
    id SERIAL PRIMARY KEY,
    unique_key BIGINT NOT NULL,
    date TIMESTAMP NOT NULL,
    is_bullish BOOL NOT NULL DEFAULT FALSE,
    is_bearish BOOL NOT NULL DEFAULT FALSE,
    direction TEXT NOT NULL,
    ce_token BIGINT NOT NULL,
    ce_beta DECIMAL(10, 2) NOT NULL,
    ce_oi DECIMAL(10, 2) NOT NULL,
    old_ce_oi DECIMAL(10, 2) NOT NULL,
    pe_token BIGINT NOT NULL,
    pe_beta DECIMAL(10, 2) NOT NULL,
    pe_oi DECIMAL(10, 2) NOT NULL,
    old_pe_oi DECIMAL(10, 2) NOT NULL,
    ce_curr_oi DECIMAL(10, 2) NOT NULL,
    pe_curr_oi DECIMAL(10, 2) NOT NULL
    );
"""

tick_details = """
    CREATE TABLE IF NOT EXISTS tick_details (
        id SERIAL PRIMARY KEY,
        token BIGINT NOT NULL,
        unique_key BIGINT NOT NULL,
        date TIMESTAMP NOT NULL,
        last_price DECIMAL(10, 2) NOT NULL,
        oi DECIMAL(10, 2) NOT NULL,
        volume_traded BIGINT NOT NULL,
        bid_volume BIGINT NOT NULL,
        offer_volume BIGINT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (token, date)
    );
    
    -- Add an index on (token, date) for faster date-based queries
    CREATE INDEX tick_details_token_date 
    ON tick_details (token, date);
"""

setting = Setting()



db = PostgresDB(setting, logging)
s = db.connect(auto = True)
print(s)
    
# db.create_database("sharemarkets")
db.create_tables(processed_details)

db.close()

# select unique_key, date, ce_token, ce_beta, ce_oi, ce_quantity, created_at from processed_details order by created_at;
# select unique_key,date,pe_token,pe_beta,pe_oi,pe_quantity,created_at from processed_details order by created_at;
#select min(date) as date1, oi from tick_details where token=13158146 group by token, oi order by date1;
