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
        gps_lat REAL,
        gps_long REAL,
        gps_speed REAL,
        gps_sat_count INTEGER,
        timebase REAL,
        status INTEGER,
        battery_remaining REAL,
        accel_x REAL,
        accel_y REAL,
        accel_z REAL,
        tape_position INTEGER,
        flow_setpoint REAL,
        flow_total REAL,
        flow1 REAL,
        flow2 REAL,
        sample_temp REAL,
        sample_rh REAL,
        sample_dewpoint REAL,
        internal_pressure REAL,
        internal_temp REAL,
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
    
    conn.commit()
    return conn

def clean_column_names(df):
    """Clean and standardize column names"""
    # Create mapping for column names to database-friendly versions
    column_mapping = {}
    
    for col in df.columns:
        # Convert to lowercase and replace spaces/special chars with underscores
        clean_name = col.lower().replace(' ', '_').replace('(', '').replace(')', '').replace('/', '_')
        clean_name = clean_name.replace('%', 'pct').replace('°', 'deg')
        
        # Handle specific column name patterns
        if 'time_utc' in clean_name:
            clean_name = 'time_utc'
        elif 'serial_number' in clean_name:
            clean_name = 'serial_number'
        elif clean_name.endswith('_c'):
            clean_name = clean_name[:-2] + '_temp'
            
        column_mapping[col] = clean_name
    
    return df.rename(columns=column_mapping)

def convert_csv_to_sqlite(csv_file_path, db_path, chunk_size=10000):
    """Convert aethalometer CSV file to SQLite database"""
    
    print(f"Converting {csv_file_path} to SQLite...")
    
    # Create or connect to database
    conn = create_aethalometer_database(db_path)
    
    try:
        # Read CSV in chunks to handle large files efficiently
        chunk_iter = pd.read_csv(csv_file_path, chunksize=chunk_size, 
                                low_memory=False, 
                                parse_dates=['Time (UTC)'],
                                date_parser=lambda x: pd.to_datetime(x, format='%Y-%m-%dT%H:%M:%SZ'))
        
        total_rows = 0
        
        for chunk_num, chunk in enumerate(chunk_iter):
            # Clean column names
            chunk = clean_column_names(chunk)
            
            # Handle any data type conversions
            if 'time_utc' in chunk.columns:
                chunk['time_utc'] = pd.to_datetime(chunk['time_utc'])
            
            # Replace 'NA' strings with None/NULL
            chunk = chunk.replace('NA', np.nan)
            
            # Insert chunk into database
            chunk.to_sql('aethalometer_data', conn, if_exists='append', 
                        index=False, method='multi')
            
            total_rows += len(chunk)
            print(f"Processed chunk {chunk_num + 1}, total rows: {total_rows}")
    
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
    
    print(f"Found {len(csv_files)} CSV files to convert")
    
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

# Example usage
if __name__ == "__main__":
    # Convert single file
    # convert_csv_to_sqlite("your_aethalometer_data.csv", "aethalometer_data.db")
    
    # Convert multiple files from a directory
    # convert_multiple_sites("/path/to/csv/files", "aethalometer_combined.db")
    
    # Example query usage after conversion:
    """
    import sqlite3
    
    conn = sqlite3.connect("aethalometer_data.db")
    
    # Query examples:
    # 1. Get hourly averages for a specific day
    query1 = '''
    SELECT 
        strftime('%H', time_utc) as hour,
        AVG(uv_bc1) as avg_uv_bc1,
        AVG(blue_bc1) as avg_blue_bc1,
        AVG(sample_temp) as avg_temp
    FROM aethalometer_data 
    WHERE date(time_utc) = '2022-04-12'
    GROUP BY hour
    ORDER BY hour
    '''
    
    # 2. Get data for specific time period
    query2 = '''
    SELECT * FROM aethalometer_data 
    WHERE time_utc BETWEEN '2022-04-12 09:00:00' AND '2022-04-12 10:00:00'
    '''
    
    # 3. Get summary statistics by site
    query3 = '''
    SELECT 
        serial_number,
        COUNT(*) as record_count,
        MIN(time_utc) as first_record,
        MAX(time_utc) as last_record,
        AVG(uv_bc1) as avg_uv_bc1
    FROM aethalometer_data 
    GROUP BY serial_number
    '''
    """
    pass