# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

from sqlalchemy import create_engine
from ETL import ETL_Pipeline
import os

paths = [r"D:\Data Analysis\E-Commerce Analytics\Datasets\amazon_fake_data",
         r"D:\Data Analysis\E-Commerce Analytics\Datasets\jumia_fake_data",
         r"D:\Data Analysis\E-Commerce Analytics\Datasets\noon_fake_data"]
db_name = 'EcommmerceDW'

def connect_to_db():
    conn_string = f'mssql+pyodbc://sa:zezo450@localhost/{db_name}?driver=ODBC+Driver+17+for+SQL+Server&timeout=0'
    engine = create_engine(conn_string, fast_executemany=True)
    return engine

def etl_pipeline():
    etl = ETL_Pipeline()
    engine = connect_to_db()
    tables_files = {"stg_products":["amazon_products.csv", "jumia_products.csv", "noon_products.csv"],
                    "stg_customers":["Amazon_customers.csv", "Jumia_customers.csv", "Noon_customers.csv"],
                    "stg_orders":["Amazon_orders.csv", "Jumia_orders.csv", "Noon_orders.csv"],
                    "stg_order_items":["Amazon_order_items.csv", "Jumia_order_items.csv", "Noon_order_items.csv"],
                    "stg_shipping":["Amazon_shipping.csv", "Jumia_shipping.csv", "Noon_shipping.csv"],
                    "stg_returns":["Amazon_returns.csv", "Jumia_returns.csv", "Noon_returns.csv"]}
    
    table_cols = {
    "stg_products": ['src_product_id', 'product_name', 'price', 'discount', 'rate', 'category', 
                     'company', 'product_page'],
    "stg_customers": ['src_customer_id', 'customer_name', 'phone', 'city', 'age', 'gender'],
    "stg_orders": ['src_order_id', 'src_customer_id', 'order_date', 'payment_method', 'city'],
    "stg_order_items":['src_order_item_id', 'src_order_id', 'src_product_id', 'product_name', 'category',
                       'company', 'quantity', 'price', 'total_price'],
    "stg_shipping":['src_shipment_id', 'src_order_id', 'src_customer_id', 'tracking_number', 'courier',
                    'shipping_status', 'shipped_date', 'delivery_date'],
    "stg_returns":['src_return_id', 'src_order_item_id', 'src_customer_id', 'return_date', 'reason',
                   'refund_amount']
    }
    
    
    for table, files in tables_files.items():
        i=0
        for file in files:
            file_path = os.path.join(paths[i], file)
            df = etl.extract_data(file_path)
            print(f"Rows in {file}: {len(df)}")
            df_transformed = etl.transform_data(df)
            df_transformed.columns = table_cols[table]
            etl.load_data_to_sql(df_transformed, table, 5000, engine)
            print(f"Processing: {file} → Loading into {table}")
            print(df_transformed.head())
            i += 1

# Run the ETL pipeline
if __name__ == "__main__":
    etl_pipeline()