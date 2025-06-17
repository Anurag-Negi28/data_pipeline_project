import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os

def generate_sales_data(num_records=1000, filename=None):
    """Generate sample sales data"""
    
    # Sample data
    products = ['Laptop', 'Mouse', 'Keyboard', 'Monitor', 'Headphones', 
                'Tablet', 'Phone', 'Camera', 'Printer', 'Speakers']
    regions = ['North', 'South', 'East', 'West', 'Central']
    sales_reps = ['Alice Johnson', 'Bob Smith', 'Charlie Brown', 'Diana Lee', 
                  'Eva Martinez', 'Frank Wilson', 'Grace Taylor', 'Henry Davis']
    
    # Generate random data
    np.random.seed(42)  # For reproducible results
    data = {
        'order_id': [f'ORD-{str(i).zfill(6)}' for i in range(1, num_records + 1)],
        'product': np.random.choice(products, num_records),
        'quantity': np.random.randint(1, 10, num_records),
        'unit_price': np.round(np.random.uniform(10, 500, num_records), 2),
        'region': np.random.choice(regions, num_records),
        'sales_rep': np.random.choice(sales_reps, num_records),
        'order_date': [
            (datetime.now() - timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d')
            for _ in range(num_records)
        ],
        'customer_id': [f'CUST-{random.randint(1000, 9999)}' for _ in range(num_records)]
    }
    
    df = pd.DataFrame(data)
    df['total_amount'] = df['quantity'] * df['unit_price']
    
    if filename:
        df.to_csv(filename, index=False)
        print(f"Generated {num_records} records in {filename}")
    
    return df

def create_sample_files():
    """Create multiple sample CSV files"""
    os.makedirs('data/input', exist_ok=True)
    
    # Generate multiple files
    for i in range(1, 4):
        filename = f'data/input/sales_data_202{i}.csv'
        generate_sales_data(500, filename)
    
    print("Sample files created successfully!")

if __name__ == "__main__":
    create_sample_files()