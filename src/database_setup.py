import sqlite3
import pandas as pd
import yaml
import os

class DatabaseManager:
    def __init__(self, config_path='config.yaml'):
        with open(config_path, 'r') as file:
            self.config = yaml.safe_load(file)
        self.db_path = self.config['database']['path']
        self.table_name = self.config['database']['table_name']
    
    def create_connection(self):
        """Create database connection"""
        # Ensure database directory exists
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
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
                source_file TEXT,
                batch_processed_date TIMESTAMP,
                stream_processed_date TIMESTAMP,
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
            # Remove any columns that don't exist in the table
            df_clean = df.copy()
            
            # Ensure required columns exist
            required_columns = ['order_id', 'product', 'quantity', 'unit_price', 
                              'total_amount', 'region', 'sales_rep', 'order_date', 'customer_id']
            
            for col in required_columns:
                if col not in df_clean.columns:
                    df_clean[col] = None
            
            # Insert sales data
            df_clean.to_sql(self.table_name, conn, if_exists='append', index=False)
            
            # Log the processing
            log_data = pd.DataFrame({
                'filename': [f'{processing_type}_processing'],
                'records_processed': [len(df)],
                'processing_type': [processing_type],
                'status': ['success']
            })
            log_data.to_sql('processing_log', conn, if_exists='append', index=False)
            
            conn.commit()
            print(f"Inserted {len(df)} records successfully via {processing_type} processing!")
            
        except Exception as e:
            print(f"Error inserting data: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    def get_stats(self):
        """Get database statistics"""
        conn = self.create_connection()
        
        try:
            # Get record count
            count_query = f"SELECT COUNT(*) as count FROM {self.table_name}"
            count_result = pd.read_sql_query(count_query, conn)
            
            # Get processing log
            log_query = "SELECT processing_type, COUNT(*) as batches, SUM(records_processed) as total_records FROM processing_log GROUP BY processing_type"
            log_result = pd.read_sql_query(log_query, conn)
            
            print(f"Total records in database: {count_result['count'][0]}")
            print("\nProcessing Statistics:")
            print(log_result.to_string(index=False))
            
        except Exception as e:
            print(f"Error getting stats: {e}")
        finally:
            conn.close()

if __name__ == "__main__":
    # Create database and tables
    db_manager = DatabaseManager()
    db_manager.create_tables()
    print("Database setup completed!")