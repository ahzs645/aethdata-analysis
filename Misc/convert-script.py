#!/usr/bin/env python3

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime
import os
import glob

def create_aethalometer_database(db_path):
    """Create SQLite database with optimized schema for aethalometer data"""
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create main data table with appropriate data types
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS aethalometer_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        serial_number TEXT NOT NULL,
        time_utc DATETIME NOT NULL,
        datum_id INTEGER,
        session_id INTEGER,
        data_format_version REAL,
        firmware_version REAL,
        app_version REAL,
        timezone_offset_mins INTEGER,
        date_local TEXT,
        time_local TEXT,
        gps_lat_ddmm_mmmmm REAL,
        gps_long_dddmm_mmmmm REAL,
        gps_speed_km_h REAL,
        gps_sat_count INTEGER,
        timebase_s REAL,
        status INTEGER,
        battery_remaining_pct REAL,
        accel_x REAL,
        accel_y REAL,
        accel_z REAL,
        tape_position INTEGER,
        flow_setpoint_ml_min REAL,
        flow_total_ml_min REAL,
        flow1_ml_min REAL,
        flow2_ml_min REAL,
        sample_temp_c REAL,
        sample_rh_pct REAL,
        sample_dewpoint_temp REAL,
        internal_pressure_pa REAL,
        internal_temp_c REAL,
        optical_config TEXT,
        
        -- UV measurements
        uv_sen1 REAL, uv_sen2 REAL, uv_ref REAL, uv_atn1 REAL, uv_atn2 REAL, uv_k REAL,
        uv_bc1 REAL, uv_bc2 REAL, uv_bcc REAL,
        
        -- Blue measurements  
        blue_sen1 REAL, blue_sen2 REAL, blue_ref REAL, blue_atn1 REAL, blue_atn2 REAL, blue_k REAL,
        blue_bc1 REAL, blue_bc2 REAL, blue_bcc REAL,
        
        -- Green measurements
        green_sen1 REAL, green_sen2 REAL, green_ref REAL, green_atn1 REAL, green_atn2 REAL, green_k REAL,
        green_bc1 REAL, green_bc2 REAL, green_bcc REAL,
        
        -- Red measurements
        red_sen1 REAL, red_sen2 REAL, red_ref REAL, red_atn1 REAL, red_atn2 REAL, red_k REAL,
        red_bc1 REAL, red_bc2 REAL, red_bcc REAL,
        
        -- IR measurements
        ir_sen1 REAL, ir_sen2 REAL, ir_ref REAL, ir_atn1 REAL, ir_atn2 REAL, ir_k REAL,
        ir_bc1 REAL, ir_bc2 REAL, ir_bcc REAL,
        
        readable_status TEXT
    )
    ''')
    
    # Create indexes for common query patterns
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_time_utc ON aethalometer_data(time_utc)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_serial_time ON aethalometer_data(serial_number, time_utc)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_date_local ON aethalometer_data(date_local)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_serial_number ON aethalometer_data(serial_number)')
    
    conn.commit()
    return conn

def clean_column_names(df):
    """Clean and standardize column names"""
    # Create mapping for column names to database-friendly versions
    column_mapping = {}
    
    for col in df.columns:
        # Convert to lowercase and replace spaces/special chars with underscores
        clean_name = col.lower().replace(' ', '_').replace('(', '').replace(')', '').replace('/', '_')
        clean_name = clean_name.replace('%', 'pct').replace('°', 'deg').replace('.', '_')
        
        # Remove extra underscores and clean up
        clean_name = '_'.join([part for part in clean_name.split('_') if part])
        
        # Handle specific column name patterns
        if 'time' in clean_name and 'utc' in clean_name:
            clean_name = 'time_utc'
        elif 'serial' in clean_name and 'number' in clean_name:
            clean_name = 'serial_number'
        elif 'date' in clean_name and 'local' in clean_name:
            clean_name = 'date_local'
        elif 'time' in clean_name and 'local' in clean_name:
            clean_name = 'time_local'
        elif clean_name.endswith('_c') and 'temp' not in clean_name:
            clean_name = clean_name[:-2] + '_temp'
        
        column_mapping[col] = clean_name
    
    return df.rename(columns=column_mapping)

def convert_csv_to_sqlite(csv_file_path, db_path, chunk_size=1000):
    """Convert aethalometer CSV file to SQLite database"""
    
    print(f"Converting {csv_file_path} to SQLite...")
    
    # Create or connect to database
    conn = create_aethalometer_database(db_path)
    
    try:
        # Read CSV in chunks to handle large files efficiently
        # First, read a small sample to check column names
        sample = pd.read_csv(csv_file_path, nrows=5)
        print(f"Found columns: {list(sample.columns)}")
        
        # Try to identify the time column
        time_col = None
        for col in sample.columns:
            if 'time' in col.lower() and 'utc' in col.lower():
                time_col = col
                break
        
        if time_col:
            chunk_iter = pd.read_csv(csv_file_path, chunksize=chunk_size, 
                                    low_memory=False)
        else:
            chunk_iter = pd.read_csv(csv_file_path, chunksize=chunk_size, 
                                    low_memory=False)
        
        total_rows = 0
        
        for chunk_num, chunk in enumerate(chunk_iter):
            # Clean column names first
            chunk = clean_column_names(chunk)
            print(f"Cleaned columns: {list(chunk.columns)}")
            
            # Handle time column conversion
            if 'time_utc' in chunk.columns:
                try:
                    chunk['time_utc'] = pd.to_datetime(chunk['time_utc'])
                except:
                    # Try different datetime formats
                    chunk['time_utc'] = pd.to_datetime(chunk['time_utc'], errors='coerce')
            
            # Replace 'NA' strings with None/NULL
            chunk = chunk.replace('NA', np.nan)
            
            # Only keep columns that exist in our schema
            expected_columns = [
                'serial_number', 'time_utc', 'datum_id', 'session_id', 'data_format_version',
                'firmware_version', 'app_version', 'timezone_offset_mins', 'date_local', 'time_local',
                'gps_lat_ddmm_mmmmm', 'gps_long_dddmm_mmmmm', 'gps_speed_km_h', 'gps_sat_count', 
                'timebase_s', 'status', 'battery_remaining_pct', 'accel_x', 'accel_y', 'accel_z', 
                'tape_position', 'flow_setpoint_ml_min', 'flow_total_ml_min', 'flow1_ml_min', 
                'flow2_ml_min', 'sample_temp_c', 'sample_rh_pct', 'sample_dewpoint_temp', 
                'internal_pressure_pa', 'internal_temp_c', 'optical_config',
                'uv_sen1', 'uv_sen2', 'uv_ref', 'uv_atn1', 'uv_atn2', 'uv_k',
                'uv_bc1', 'uv_bc2', 'uv_bcc', 'blue_sen1', 'blue_sen2', 'blue_ref',
                'blue_atn1', 'blue_atn2', 'blue_k', 'blue_bc1', 'blue_bc2', 'blue_bcc',
                'green_sen1', 'green_sen2', 'green_ref', 'green_atn1', 'green_atn2', 'green_k',
                'green_bc1', 'green_bc2', 'green_bcc', 'red_sen1', 'red_sen2', 'red_ref',
                'red_atn1', 'red_atn2', 'red_k', 'red_bc1', 'red_bc2', 'red_bcc',
                'ir_sen1', 'ir_sen2', 'ir_ref', 'ir_atn1', 'ir_atn2', 'ir_k',
                'ir_bc1', 'ir_bc2', 'ir_bcc', 'readable_status'
            ]
            
            # Only keep columns that exist in both the chunk and our expected schema
            chunk = chunk[[col for col in chunk.columns if col in expected_columns]]
            
            # Insert chunk into database using smaller batches to avoid SQL variable limit
            chunk.to_sql('aethalometer_data', conn, if_exists='append', 
                        index=False, method=None, chunksize=500)
            
            total_rows += len(chunk)
            print(f"Processed chunk {chunk_num + 1}, total rows: {total_rows}")
            
            # Only show this for first chunk to avoid spam
            if chunk_num == 0:
                print(f"Sample of processed columns: {list(chunk.columns)[:10]}")
    
    except Exception as e:
        print(f"Error processing {csv_file_path}: {e}")
        return False
    
    finally:
        conn.close()
    
    print(f"Successfully converted {csv_file_path} - {total_rows} rows")
    return True

def convert_multiple_sites(csv_directory, output_db_path):
    """Convert multiple CSV files from different sites into one database"""
    
    csv_files = glob.glob(os.path.join(csv_directory, "*.csv"))
    
    if not csv_files:
        print(f"No CSV files found in {csv_directory}")
        return
    
    print(f"Found {len(csv_files)} CSV files to convert:")
    for f in csv_files:
        print(f"  - {os.path.basename(f)}")
    
    # Remove existing database if it exists
    if os.path.exists(output_db_path):
        os.remove(output_db_path)
    
    success_count = 0
    for csv_file in csv_files:
        if convert_csv_to_sqlite(csv_file, output_db_path):
            success_count += 1
    
    print(f"\nConversion complete: {success_count}/{len(csv_files)} files successful")
    
    # Print database statistics
    conn = sqlite3.connect(output_db_path)
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM aethalometer_data")
    total_rows = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(DISTINCT serial_number) FROM aethalometer_data")
    unique_sites = cursor.fetchone()[0]
    
    cursor.execute("SELECT MIN(time_utc), MAX(time_utc) FROM aethalometer_data")
    date_range = cursor.fetchone()
    
    print(f"\nDatabase Statistics:")
    print(f"Total records: {total_rows:,}")
    print(f"Unique sites: {unique_sites}")
    print(f"Date range: {date_range[0]} to {date_range[1]}")
    
    # Show file size comparison
    db_size_mb = os.path.getsize(output_db_path) / (1024 * 1024)
    print(f"Database size: {db_size_mb:.1f} MB")
    
    conn.close()

# Run the conversion
if __name__ == "__main__":
    convert_multiple_sites(
        "/Users/ahzs645/Library/CloudStorage/GoogleDrive-ahzs645@gmail.com/My Drive/University/Research/Grad/UC Davis Ann/NASA MAIA/Data/Aethelometry Data/untitled folder",
        "/Users/ahzs645/Library/CloudStorage/GoogleDrive-ahzs645@gmail.com/My Drive/University/Research/Grad/UC Davis Ann/NASA MAIA/Data/Aethelometry Data/aethalometer_combined.db"
    )