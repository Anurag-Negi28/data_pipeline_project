import sqlite3
import pandas as pd
import yaml

class DatabaseManager:
    def __init__(self, config_path='config.yaml'):
        with open(config_path, 'r') as file:
            self.config = yaml.safe_load(file)
        self.db_path = self.config['database']['path']
        self.table_name = self.config['database']['table_name']
    
    def create_connection(self):
        """Create database connection"""
        return sqlite3.connect(self.db_path)
    
    def create_tables(self):
        """Create necessary tables"""
        conn = self.create_connection()
        cursor = conn.cursor()
        
        # Create sales_records table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sales_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id TEXT UNIQUE,
                product TEXT,
                quantity INTEGER,
                unit_price REAL,
                total_amount REAL,
                region TEXT,
                sales_rep TEXT,
                order_date DATE,
                customer_id TEXT,
                processed_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create processing_log table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS processing_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filename TEXT,
                records_processed INTEGER,
                processing_type TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status TEXT
            )
        ''')
        
        conn.commit()
        conn.close()
        print("Database tables created successfully!")
    
    def insert_data(self, df, processing_type="batch"):
        """Insert DataFrame into database"""
        conn = self.create_connection()
        
        try:
            # Insert sales data
            df.to_sql(self.table_name, conn, if_exists='append', index=False)
            
            # Log the processing
            log_data = pd.DataFrame({
                'filename': ['batch_processing'],
                'records_processed': [len(df)],
                'processing_type': [processing_type],
                'status': ['success']
            })
            log_data.to_sql('processing_log', conn, if_exists='append', index=False)
            
            conn.commit()
            print(f"Inserted {len(df)} records successfully!")
            
        except Exception as e:
            print(f"Error inserting data: {e}")
            conn.rollback()
        finally:
            conn.close()