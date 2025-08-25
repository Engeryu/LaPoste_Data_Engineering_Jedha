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
    """Enriches DataFrame with time-based features.

    Adds 'Weekday', 'Hour', and 'Day_Type' columns based on 'Pickup_DateTime'.

    Args:
        df (pd.DataFrame): The input DataFrame with a 'Pickup_DateTime' column.

    Returns:
        pd.DataFrame: The DataFrame enriched with time features.
    """
    df['Pickup_DateTime'] = pd.to_datetime(df['Pickup_DateTime'])
    df['Weekday'] = df['Pickup_DateTime'].dt.day_name()
    df['Hour'] = df['Pickup_DateTime'].dt.hour
    df['Day_Type'] = np.where(df['Weekday'].isin(['Saturday', 'Sunday']), 'Weekend', 'Weekday')
    return df

def _enrich_with_weather(df: pd.DataFrame) -> pd.DataFrame:
    """Enriches DataFrame with weather data from a JSON file.

    Maps pickup times to hourly weather conditions. A 'Weather_Key' is created
    from the timestamp for lookup. Unmatched records get 'Unknown' status.

    Args:
        df (pd.DataFrame): DataFrame with a 'Pickup_DateTime' column.

    Returns:
        pd.DataFrame: The DataFrame enriched with a 'Weather_Condition' column.

    Raises:
        FileNotFoundError: If the weather JSON file is not found.
        json.JSONDecodeError: If the weather JSON file is malformed.
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

    Applies a sequence of enrichment and business logic steps:
    1. Adds time-based features.
    2. Enriches with weather conditions.
    3. Calculates delivery status using business rules.
    4. Selects and renames final columns.

    Args:
        df (pd.DataFrame): The raw DataFrame extracted from the source.

    Returns:
        pd.DataFrame: A clean, analysis-ready DataFrame.
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
    """Extracts the 'deliveries' table from the source SQLite database.

    This function represents the 'Extract' step of the ETL process.

    Returns:
        pd.DataFrame: A DataFrame containing all records from the 'deliveries' table.

    Raises:
        FileNotFoundError: If the database file does not exist.
        sqlite3.Error: If a database error occurs during extraction.
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

    This function represents the 'Load' step of the ETL process. It supports
    'csv', 'parquet', 'json', 'db', and 'xlsx' formats.

    Args:
        df (pd.DataFrame): The final, transformed DataFrame to be saved.
        output_format (str): The target file format.

    Raises:
        Exception: Propagates any exceptions that occur during file writing.
    """
    output_path = f"{config.OUTPUT_FILENAME_BASE}.{output_format}"
    config.logger.info(f"Loading final data to '{output_path}'...")
    try:
        if output_format == 'csv': df.to_csv(output_path, index=False)
        elif output_format == 'parquet': df.to_parquet(output_path, index=False, engine='pyarrow', compression='snappy')
        elif output_format == 'json': df.to_json(output_path, orient='records', indent=4)
        elif output_format == 'xlsx':
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
