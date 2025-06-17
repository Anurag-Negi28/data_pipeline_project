import sys
import os
import pandas as pd
import sqlite3
import glob
import yaml
import logging
from datetime import datetime
import shutil

# Add the parent directory to the Python path so we can import from src
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database_setup import DatabaseManager

class BatchETLPipeline:
    def __init__(self, config_path='config.yaml'):
        # Load configuration
        with open(config_path, 'r') as file:
            self.config = yaml.safe_load(file)
        
        self.db_manager = DatabaseManager(config_path)
        self.setup_logging()
    
    def setup_logging(self):
        """Setup logging configuration"""
        log_dir = self.config['paths']['log_dir']
        os.makedirs(log_dir, exist_ok=True)
        
        logging.basicConfig(
            level=getattr(logging, self.config['logging']['level']),
            format=self.config['logging']['format'],
            handlers=[
                logging.FileHandler(f"{log_dir}/batch_pipeline.log"),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def extract(self):
        """Extract data from CSV files"""
        input_dir = self.config['paths']['input_dir']
        csv_files = glob.glob(f"{input_dir}/*.csv")
        
        if not csv_files:
            self.logger.info("No CSV files found for processing")
            return pd.DataFrame()
        
        dataframes = []
        processed_files = []
        
        for file_path in csv_files:
            try:
                df = pd.read_csv(file_path)
                df['source_file'] = os.path.basename(file_path)
                dataframes.append(df)
                processed_files.append(file_path)
                self.logger.info(f"Extracted {len(df)} records from {file_path}")
            except Exception as e:
                self.logger.error(f"Error reading {file_path}: {e}")
        
        if dataframes:
            combined_df = pd.concat(dataframes, ignore_index=True)
            self.processed_files = processed_files
            return combined_df
        
        return pd.DataFrame()
    
    def transform(self, df):
        """Transform and clean the data"""
        if df.empty:
            return df
        
        self.logger.info(f"Starting transformation of {len(df)} records")
        
        # Data cleaning and transformation
        try:
            # Remove duplicates based on order_id
            initial_count = len(df)
            df = df.drop_duplicates(subset=['order_id'], keep='first')
            self.logger.info(f"Removed {initial_count - len(df)} duplicate records")
            
            # Clean and validate data
            df = df.dropna(subset=['order_id', 'product', 'quantity', 'unit_price'])
            
            # Data type conversions
            df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
            df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce')
            df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
            
            # Calculate total_amount if not present or incorrect
            df['total_amount'] = df['quantity'] * df['unit_price']
            
            # Add processing metadata
            df['batch_processed_date'] = datetime.now()
            
            # Remove rows with invalid data
            df = df.dropna(subset=['quantity', 'unit_price', 'order_date'])
            
            self.logger.info(f"Transformation completed. Final record count: {len(df)}")
            
        except Exception as e:
            self.logger.error(f"Error during transformation: {e}")
            raise
        
        return df
    
    def load(self, df):
        """Load data into SQLite database"""
        if df.empty:
            self.logger.info("No data to load")
            return
        
        try:
            self.db_manager.insert_data(df, processing_type="batch")
            self.logger.info(f"Successfully loaded {len(df)} records to database")
        except Exception as e:
            self.logger.error(f"Error loading data: {e}")
            raise
    
    def archive_files(self):
        """Move processed files to archive"""
        archive_dir = self.config['paths']['archive_dir']
        os.makedirs(archive_dir, exist_ok=True)
        
        for file_path in getattr(self, 'processed_files', []):
            try:
                filename = os.path.basename(file_path)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                archived_name = f"{timestamp}_{filename}"
                shutil.move(file_path, f"{archive_dir}/{archived_name}")
                self.logger.info(f"Archived {filename} as {archived_name}")
            except Exception as e:
                self.logger.error(f"Error archiving {file_path}: {e}")
    
    def run_pipeline(self):
        """Execute the complete ETL pipeline"""
        self.logger.info("Starting batch ETL pipeline")
        
        try:
            # Extract
            raw_data = self.extract()
            
            if raw_data.empty:
                self.logger.info("No data to process")
                return
            
            # Transform
            cleaned_data = self.transform(raw_data)
            
            # Load
            self.load(cleaned_data)
            
            # Archive processed files
            self.archive_files()
            
            self.logger.info("Batch ETL pipeline completed successfully")
            
        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}")
            raise

if __name__ == "__main__":
    pipeline = BatchETLPipeline()
    pipeline.run_pipeline()