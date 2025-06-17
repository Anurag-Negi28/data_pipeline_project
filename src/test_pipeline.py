import sys
import os
import pandas as pd
import sqlite3

# Add the parent directory to the Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data_generator import generate_sales_data
from src.database_setup import DatabaseManager

def test_batch_pipeline():
    """Test the batch pipeline"""
    print("Testing Batch Pipeline...")
    
    # Generate test data
    os.makedirs('data/input', exist_ok=True)
    generate_sales_data(100, 'data/input/test_batch.csv')
    
    # Run batch pipeline
    from src.batch_pipeline import BatchETLPipeline
    pipeline = BatchETLPipeline()
    pipeline.run_pipeline()
    
    # Verify data in database
    db_manager = DatabaseManager()
    conn = db_manager.create_connection()
    
    try:
        result = pd.read_sql_query("SELECT COUNT(*) as count FROM sales_records", conn)
        print(f"Records in database: {result['count'][0]}")
    except Exception as e:
        print(f"Error checking database: {e}")
    finally:
        conn.close()
    
    print("Batch pipeline test completed!")

def test_stream_pipeline():
    """Test the stream pipeline"""
    print("Testing Stream Pipeline...")
    print("To test stream pipeline:")
    print("1. Start the stream pipeline in another terminal:")
    print("   python -m src.stream_pipeline")
    print("2. Then add a test file by running:")
    print("   python -c \"from src.data_generator import generate_sales_data; generate_sales_data(50, 'data/input/test_stream.csv')\"")
    
    # Generate test data for streaming
    os.makedirs('data/input', exist_ok=True)
    generate_sales_data(50, 'data/input/test_stream_ready.csv')
    print("Test stream file 'test_stream_ready.csv' created!")
    print("You can copy this file to data/input/ while stream pipeline is running to test it.")

def view_database_stats():
    """View database statistics"""
    try:
        db_manager = DatabaseManager()
        conn = db_manager.create_connection()
        
        # Check if tables exist
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        if not tables:
            print("No tables found. Please run database setup first:")
            print("python -m src.database_setup")
            return
        
        print(f"Available tables: {[table[0] for table in tables]}")
        
        # Sales records stats
        print("\n=== DATABASE STATISTICS ===")
        
        # Check if sales_records table has data
        try:
            count_result = pd.read_sql_query("SELECT COUNT(*) as count FROM sales_records", conn)
            total_records = count_result['count'][0]
            
            if total_records == 0:
                print("No records found in sales_records table.")
                print("Run some pipeline tests first to populate data.")
            else:
                stats_query = """
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT product) as unique_products,
                    COUNT(DISTINCT region) as unique_regions,
                    ROUND(AVG(total_amount), 2) as avg_order_value,
                    MIN(order_date) as earliest_order,
                    MAX(order_date) as latest_order
                FROM sales_records
                """
                
                stats = pd.read_sql_query(stats_query, conn)
                print(stats.to_string(index=False))
        
        except Exception as e:
            print(f"Error querying sales_records: {e}")
        
        # Processing log
        try:
            print("\n=== PROCESSING LOG ===")
            log_query = "SELECT * FROM processing_log ORDER BY timestamp DESC LIMIT 10"
            log_data = pd.read_sql_query(log_query, conn)
            
            if log_data.empty:
                print("No processing logs found.")
            else:
                print(log_data.to_string(index=False))
        
        except Exception as e:
            print(f"Error querying processing_log: {e}")
        
        conn.close()
        
    except Exception as e:
        print(f"Error connecting to database: {e}")
        print("Make sure to run: python -m src.database_setup")

def setup_test_environment():
    """Setup test environment"""
    print("Setting up test environment...")
    
    # Create database and tables
    db_manager = DatabaseManager()
    db_manager.create_tables()
    
    # Create directories
    os.makedirs('data/input', exist_ok=True)
    os.makedirs('data/processed', exist_ok=True)
    os.makedirs('data/archive', exist_ok=True)
    os.makedirs('logs', exist_ok=True)
    
    print("Test environment setup completed!")

if __name__ == "__main__":
    print("=== DATA PIPELINE TEST SUITE ===")
    print("Select test option:")
    print("1. Test Batch Pipeline")
    print("2. Test Stream Pipeline")  
    print("3. View Database Stats")
    print("4. Setup Test Environment")
    
    choice = input("Enter choice (1-4): ")
    
    if choice == '1':
        test_batch_pipeline()
    elif choice == '2':
        test_stream_pipeline()
    elif choice == '3':
        view_database_stats()
    elif choice == '4':
        setup_test_environment()
    else:
        print("Invalid choice")