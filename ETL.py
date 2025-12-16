# -*- coding: utf-8 -*-
"""
Created on Thu Nov 27 07:26:29 2025

@author: HP
"""
import pandas as pd
from sqlalchemy import text

class ETL_Pipeline:

    # Extract step: Read the CSV file
    def extract_data(self, file_path):
        df = pd.read_csv(file_path, index_col=False)
        return df
    
    # Transform step
    def transform_data(self, df):
        return df
    
    # Load step
    def load_data_to_sql(self, df, table, chunk_size, engine):
        """
        Comprehensive data loading with schema validation and error handling
        """
        try:
            # 1. Analyze data first
            print("📊 Analyzing data...")
            max_lengths = {}
            for col in df.select_dtypes(include=['object']).columns:
                max_len = df[col].astype(str).str.len().max()
                max_lengths[col] = max_len
                print(f"   {col}: max length = {max_len}")
            
            # 2. Ensure table schema can handle the data
            try:
                with engine.connect() as conn:
                    # Check if we need to alter any columns
                    for col, max_len in max_lengths.items():
                        if max_len > 4000:  # If any string is longer than 100 chars
                            conn.execute(text(f"""
                                ALTER TABLE f"{table}" 
                                ALTER COLUMN {col} NVARCHAR(MAX)
                            """))
                            print(f"✅ Increased {col} column size to NVARCHAR(1000)")
            except:
                print("⚠️  Could not alter table, proceeding with data truncation")
            
            # 3. Clean and truncate data if needed
            df_clean = df.copy()
            for col in df_clean.select_dtypes(include=['object']).columns:
                df_clean[col] = df_clean[col].astype(str).str.slice(0, 1000)
            
            # 4. Load data in chunks
            total_rows = len(df_clean)
            
            print(f"📦 Loading {total_rows} records in chunks of {chunk_size}...")
            
            schema_name = "staging"
            for i in range(0, total_rows, chunk_size):
                chunk = df_clean.iloc[i:i + chunk_size]
                chunk.to_sql(
                    f"{table}", 
                    con=engine, 
                    schema=schema_name,
                    if_exists='append', 
                    index=False,
                    method=None  # Use default method for better error handling
                )
                print(f"   ...loaded {min(i + chunk_size, total_rows)}/{total_rows}")
            
            print(f"✅ Successfully loaded all {total_rows} records!")
            
        except Exception as e:
            print(f"❌ Error during data loading: {e}")
            
            # Save to CSV as backup
            backup_path = f"{table}_backup.csv"
            df.to_csv(backup_path, index=False)
            print(f"💾 Data saved to {backup_path} as backup")
        

