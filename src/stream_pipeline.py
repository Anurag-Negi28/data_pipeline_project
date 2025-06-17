import sys
import os
import pandas as pd
import time
import yaml
import logging
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Add the parent directory to the Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database_setup import DatabaseManager

class StreamFileHandler(FileSystemEventHandler):
    def __init__(self, stream_processor):
        self.stream_processor = stream_processor
    
    def on_created(self, event):
        """Handle new file creation"""
        if event.is_directory:
            return
        
        # Only process CSV files
        if event.src_path.endswith('.csv'):
            # Wait a moment to ensure file is completely written
            time.sleep(1)
            self.stream_processor.process_file(event.src_path)

class StreamETLPipeline:
    def __init__(self, config_path='config.yaml'):
        # Load configuration
        with open(config_path, 'r') as file:
            self.config = yaml.safe_load(file)
        
        self.db_manager = DatabaseManager(config_path)
        self.setup_logging()
        self.processed_count = 0
    
    def setup_logging(self):
        """Setup logging configuration"""
        log_dir = self.config['paths']['log_dir']
        os.makedirs(log_dir, exist_ok=True)
        
        logging.basicConfig(
            level=getattr(logging, self.config['logging']['level']),
            format=self.config['logging']['format'],
            handlers=[
                logging.FileHandler(f"{log_dir}/stream_pipeline.log"),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def transform_record(self, record):
        """Transform a single record"""
        try:
            # Clean and validate individual record
            if pd.isna(record.get('order_id')) or pd.isna(record.get('product')):
                return None
            
            # Data type conversions
            record['quantity'] = pd.to_numeric(record['quantity'], errors='coerce')
            record['unit_price'] = pd.to_numeric(record['unit_price'], errors='coerce')
            
            if pd.isna(record['quantity']) or pd.isna(record['unit_price']):
                return None
            
            # Calculate total amount
            record['total_amount'] = record['quantity'] * record['unit_price']
            record['stream_processed_date'] = datetime.now()
            
            return record
        except Exception as e:
            self.logger.error(f"Error transforming record: {e}")
            return None
    
    def process_file(self, file_path):
        """Process a single CSV file in streaming fashion"""
        self.logger.info(f"Processing new file: {file_path}")
        
        try:
            # Read CSV file
            df = pd.read_csv(file_path)
            
            # Process records one by one (simulating streaming)
            valid_records = []
            
            for _, row in df.iterrows():
                transformed_record = self.transform_record(row.to_dict())
                if transformed_record:
                    valid_records.append(transformed_record)
            
            if valid_records:
                # Convert to DataFrame and insert
                processed_df = pd.DataFrame(valid_records)
                self.db_manager.insert_data(processed_df, processing_type="stream")
                
                self.processed_count += len(processed_df)
                self.logger.info(f"Processed {len(processed_df)} records from {file_path}")
                self.logger.info(f"Total processed so far: {self.processed_count}")
            
            # Move processed file
            self.archive_processed_file(file_path)
            
        except Exception as e:
            self.logger.error(f"Error processing file {file_path}: {e}")
    
    def archive_processed_file(self, file_path):
        """Move processed file to archive"""
        try:
            processed_dir = self.config['paths']['processed_dir']
            os.makedirs(processed_dir, exist_ok=True)
            
            filename = os.path.basename(file_path)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            new_path = f"{processed_dir}/{timestamp}_{filename}"
            
            os.rename(file_path, new_path)
            self.logger.info(f"Moved {filename} to processed directory")
            
        except Exception as e:
            self.logger.error(f"Error moving file {file_path}: {e}")
    
    def start_monitoring(self):
        """Start monitoring the input directory for new files"""
        input_dir = self.config['paths']['input_dir']
        os.makedirs(input_dir, exist_ok=True)
        
        self.logger.info(f"Starting stream processing. Monitoring: {input_dir}")
        
        event_handler = StreamFileHandler(self)
        observer = Observer()
        observer.schedule(event_handler, input_dir, recursive=False)
        
        observer.start()
        
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.logger.info("Stopping stream processing...")
            observer.stop()
        
        observer.join()
        self.logger.info("Stream processing stopped")

if __name__ == "__main__":
    stream_pipeline = StreamETLPipeline()
    stream_pipeline.start_monitoring()