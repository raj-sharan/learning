import psycopg2
import pandas as pd
import numpy as np
import warnings
from psycopg2 import sql
import psycopg2.extras as extras

warnings.filterwarnings("ignore")

class PostgresDB:
    def __init__(self, setting, logging):
        """Initialize the database connection."""
        self.db_name     = setting.db_name
        self.db_user     = setting.db_username
        self.db_password = setting.db_password
        self.db_host     = setting.db_host
        self.db_port     = setting.db_port
        self.conn        = None
        self.cur         = None
        self.logging     = logging

    def connect(self, auto = False):
        """Establish a connection to the PostgreSQL database."""
        try:
            self.conn = psycopg2.connect(
                dbname   = self.db_name,
                user     = self.db_user,
                password = self.db_password,
                host     = self.db_host,
                port     = self.db_port
            )
            if auto:
                self.conn.autocommit = True
            self.cur = self.conn.cursor()
            self.logging.debug("✅ Connected to PostgreSQL successfully!")
        except Exception as e:
            self.logging.debug(f"❌ Error connecting to the database: {e}")
            return None

        return self.conn
        
    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()
        
    def execute_query(self, query, values=None):
        """Execute a SQL query (SELECT, INSERT, UPDATE, DELETE)."""
        try:
            if self.cur is None:
                self.logging.debug("⚠️ No active database connection.")
                return False
                
            self.cur.execute(query, values or ())
        
            self.logging.debug("✅ Query executed successfully.")
        except Exception as e:
            self.logging.debug(f"❌ Error executing query: {e}")
            return False
            
        return True

    def insert_bulk_data(self, query, batch_data):
        try:       
            if self.cur is None:
                self.logging.debug("⚠️ No active database connection.")
                return False

            # print(query)
            # print(batch_data)
            extras.execute_values(self.cur, query, batch_data) 
        
            self.logging.debug("✅ Query executed successfully.")
        except Exception as e:
            self.logging.debug(f"❌ Error executing query: {e}")
            return False
            
        return True

    
    def fetch_records(self, query):
        try:
            # Execute the query
            self.cur.execute(query)
            
            # Fetch all records
            return self.cur.fetchall()
        except Exception as e:
            self.logging.debug(f"❌ Error fetching records: {e}")

        return None

    def get_records_in_data_frame(self, query):
        try:
            if self.cur is None:
                self.logging.debug("❌ No active database connection.")
                return None

            # Fetch data into a Pandas DataFrame
            return pd.read_sql(query, self.conn)
            
        except Exception as e:
            self.logging.debug(f"❌ failed to load records: {e}")
            return None

    def close(self):
        """Close the database connection."""
        if self.cur is not None:
            self.cur.close()
        if self.conn is not None:
            self.conn.close()
        self.logging.debug("🔌 Database connection closed.")

    def create_database(self, db_name):
        """Creates the 'sharemarkets' database if it doesn't exist."""
        try:
            if self.cur is None:
                self.logging.debug("❌ No active database connection.")
                return False
            
            # Create database if it does not exist
            self.cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{db_name}'")
            if not self.cur.fetchone():
                self.cur.execute(f"CREATE DATABASE {db_name}")
                # self.db_conn.commit()
                self.logging.debug(f"✅ Database '{db_name}' created successfully.")
            else:
                self.logging.debug(f"✅ Database '{db_name}' already exists.")
        except Exception as e:
            self.logging.debug(f"❌ Error creating database: {e}")
            return False

        return True

    def create_tables(self, query):
        """Creates tables in the 'sharemarkets' database."""
        try:
            if self.cur is None:
                self.logging.debug("❌ No active database connection.")
                return False
    
            # Execute table creation queries
            self.cur.execute(query)
    
            self.logging.debug(f"✅ Table created successfully.")
        except Exception as e:
            self.logging.debug(f"❌ Error creating tables: {e}")
            return False

        return True