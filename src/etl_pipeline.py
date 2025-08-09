# supercourier_etl/etl_pipeline.py
"""
Core ETL (Extract, Transform, Load) logic module.
Handles data extraction from sources, applies transformations and business rules,
and loads the final dataset into various formats.
"""

# Imports of the necessary libraries
import numpy as np
import pandas as pd
import json
import os
import sqlite3
# Imports from the local modules
from . import config, domain

def _add_datetime_features(df: pd.DataFrame) -> pd.DataFrame:
    """Enriches the DataFrame with time-based features from the 'Pickup_DateTime' column.
    This function adds the following new columns:
    - Weekday: The name of the day (e.g., 'Monday').
    - Hour: The hour of the day (0-23).
    - Day_Type: Categorizes the day as either 'Weekday' or 'Weekend'.
    """
    df['Pickup_DateTime'] = pd.to_datetime(df['Pickup_DateTime'])
    df['Weekday'] = df['Pickup_DateTime'].dt.day_name()
    df['Hour'] = df['Pickup_DateTime'].dt.hour
    df['Day_Type'] = np.where(df['Weekday'].isin(['Saturday', 'Sunday']), 'Weekend', 'Weekday')
    return df

def _enrich_with_weather(df: pd.DataFrame) -> pd.DataFrame:
    """Enriches the DataFrame with weather data by mapping pickup times to hourly conditions.
    It reads weather data from the JSON file specified in the configuration.
    A 'Weather_Key' is created from the timestamp to look up the corresponding condition.
    If a match is not found, the condition is set to 'Unknown' and a warning is logged.
    """
    try:
        with open(config.WEATHER_PATH, 'r') as f:
            weather_data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        config.logger.error(f"Could not load or parse weather data: {e}")
        raise
    df['Weather_Key'] = df['Pickup_DateTime'].dt.strftime('%Y-%m-%d-%H')
    df['Weather_Condition'] = df['Weather_Key'].map(weather_data).fillna('Unknown')
    if 'Unknown' in df['Weather_Condition'].unique():
        config.logger.warning("Some records could not be matched with weather data.")
    return df

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """Orchestrates the full data transformation pipeline.
    This function applies a series of enrichment and business logic steps in a specific order:
    1. Adds time-based features using `_add_datetime_features`.
    2. Enriches the data with weather conditions using `_enrich_with_weather`.
    3. Applies core business rules to calculate delivery status via `domain.calculate_delivery_status`.
    4. Selects and renames the final columns for the output dataset.
    Returns a clean, analysis-ready DataFrame.
    """
    config.logger.info("Starting data transformation pipeline...")
    
    df_transformed = _add_datetime_features(df)
    df_transformed = _enrich_with_weather(df_transformed)
    df_final_logic = domain.calculate_delivery_status(df_transformed)

    minutes = df_final_logic['Actual_Delivery_Time_Minutes'].astype(int)
    seconds = ((df_final_logic['Actual_Delivery_Time_Minutes'] - minutes) * 60).astype(int)
    df_final_logic['Actual_Delivery_Time'] = minutes.astype(str).str.zfill(2) + '.' + seconds.astype(str).str.zfill(2)

    final_cols = [
        'Delivery_ID', 'Pickup_DateTime', 'Weekday', 'Hour', 'Package_Type', 'Distance',
        'Delivery_Zone', 'Weather_Condition', 'Actual_Delivery_Time', 'Status'
    ]
    final_df = df_final_logic[final_cols].copy()
    
    config.logger.info("Data transformation complete.")
    return final_df

def extract_data_from_sqlite() -> pd.DataFrame:
    """Extracts the complete 'deliveries' table from the source SQLite database.
    This function represents the 'Extract' step of the ETL process.
    It connects to the database path defined in the configuration and loads the data into a pandas DataFrame.
    Raises a FileNotFoundError if the database file does not exist.
    """
    config.logger.info(f"Extracting data from '{config.DB_PATH}'...")
    if not os.path.exists(config.DB_PATH):
        config.logger.error(f"Database file not found at '{config.DB_PATH}'.")
        raise FileNotFoundError(f"Database file not found at '{config.DB_PATH}'.")
    conn = None
    try:
        conn = sqlite3.connect(config.DB_PATH)
        df = pd.read_sql_query("SELECT * FROM deliveries", conn)
        config.logger.info(f"Successfully extracted {len(df)} records from the database.")
        return df
    except sqlite3.Error as e:
        config.logger.error(f"Failed to extract data from SQLite: {e}")
        raise
    finally:
        if conn:
            conn.close()

def load_data(df: pd.DataFrame, output_format: str) -> None:
    """Saves the final transformed DataFrame to a specified file format.
    This function represents the 'Load' step of the ETL process.
    It handles the logic for writing to various formats based on user selection:
    - csv: Comma-Separated Values.
    - parquet: Compressed, columnar format ideal for big data.
    - json: Human-readable format for web applications.
    - db: A new SQLite database table.
    - xlsx: Excel spreadsheet, optimized for large files using `constant_memory` mode.
    """
    output_path = f"{config.OUTPUT_FILENAME_BASE}.{output_format}"
    config.logger.info(f"Loading final data to '{output_path}'...")
    try:
        if output_format == 'csv': df.to_csv(output_path, index=False)
        elif output_format == 'parquet': df.to_parquet(output_path, index=False, engine='pyarrow', compression='snappy')
        elif output_format == 'json': df.to_json(output_path, orient='records', indent=4)
        elif output_format == 'xlsx':
            # Use xlsxwriter's constant_memory mode for performance with large files.
            # This reduces memory usage by writing data to temporary files on disk.
            with pd.ExcelWriter(output_path, engine='xlsxwriter', engine_kwargs={'options': {'constant_memory': True}}) as writer:
                df.to_excel(writer, index=False, sheet_name='Deliveries Analysis')

        elif output_format == 'db':
            conn = sqlite3.connect(output_path)
            df.to_sql('deliveries_analysis', conn, if_exists='replace', index=False)
            conn.close()
        else:
            config.logger.error(f"Unsupported format '{output_format}'.")
            return
        config.logger.info(f"Successfully saved the final dataset to '{output_path}'.")
    except Exception as e:
        config.logger.error(f"Failed to write data to {output_format} format: {e}")
        raise
