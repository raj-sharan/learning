#!/Users/grajwade/vPython/bin/python

from settings import Setting
from db_connect import PostgresDB

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

setting = Setting()



db = PostgresDB(setting)
s = db.connect()
print(s)
    
# db.create_database("sharemarkets")
db.create_tables(historical_test_data_5m_sql)

db.close()

