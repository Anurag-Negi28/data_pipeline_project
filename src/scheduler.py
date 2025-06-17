import sys
import os
import schedule
import time
import logging
from datetime import datetime

# Add the parent directory to the Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.batch_pipeline import BatchETLPipeline

def run_batch_job():
    """Run the batch ETL pipeline"""
    print(f"\n{'='*50}")
    print(f"Running batch job at: {datetime.now()}")
    print(f"{'='*50}")
    
    try:
        pipeline = BatchETLPipeline()
        pipeline.run_pipeline()
    except Exception as e:
        logging.error(f"Batch job failed: {e}")

def main():
    """Main scheduler function"""
    print("Starting ETL Scheduler...")
    print("Batch jobs will run every 5 minutes")
    print("Press Ctrl+C to stop")
    
    # Schedule batch job every 5 minutes
    schedule.every(5).minutes.do(run_batch_job)
    
    # Run initial batch job
    run_batch_job()
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nScheduler stopped")

if __name__ == "__main__":
    main()