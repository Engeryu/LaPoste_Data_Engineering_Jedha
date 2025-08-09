# supercourier_etl/data_generators.py
"""
Contains functions to generate realistic source data for the ETL pipeline,
including a SQLite database for deliveries and a JSON file for weather conditions.
"""

# Imports of the necessary libraries
import sqlite3
import json
import random
from datetime import datetime, timedelta
# Imports from the local modules
from . import config, domain

#==================================================================================
# --- Basic SQL Database Generation Functions ---
#==================================================================================

def generate_sqlite_database(rows_deliveries: int):
    """
    Generates a SQLite database with simulated delivery tracking data.\n
    This function creates the primary data source for the ETL pipeline.\n
    It populates a 'deliveries' table with a specified number of records.\n
    The table includes the following fields:\n
    - Delivery_ID\n
    - Pickup_DateTime\n
    - Package_Type\n
    - Distance\n
    - Delivery_Zone\n
    - Actual_Delivery_Time_Minutes\n
    Data values are randomized but weighted using coefficients from the `domain` module to ensure realism.\n
    """
    config.logger.info(f"Generating SQLite database at '{config.DB_PATH}' with {rows_deliveries} records...")
    conn = None
    try:
        conn = sqlite3.connect(config.DB_PATH)
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS deliveries")
        cursor.execute("""
            CREATE TABLE deliveries (
                Delivery_ID TEXT PRIMARY KEY, Pickup_DateTime TEXT, Package_Type TEXT,
                Distance REAL, Delivery_Zone TEXT, Actual_Delivery_Time_Minutes REAL
            )""")
        deliveries_data = []
        start_date = datetime.now() - timedelta(days=config.MIN_NUM_WEATHER_DAYS)
        for i in range(rows_deliveries):
            pickup_time = start_date + timedelta(seconds=random.randint(0, config.MIN_NUM_WEATHER_DAYS * 86400))
            distance = round(random.uniform(1.0, 75.0), 2)
            base_time = 20 + distance * 1.5
            random_delay = random.uniform(-10, 45)
            actual_time = round(max(15, base_time + random_delay), 2)
            deliveries_data.append((
                f"SC-DEL-{i+1:05d}",
                pickup_time.strftime('%Y-%m-%d %H:%M:%S'),
                random.choice(list(domain.PACKAGE_TYPE_COEFFS.keys())),
                distance,
                random.choice(list(domain.DELIVERY_ZONE_COEFFS.keys())),
                actual_time
            ))
        cursor.executemany("INSERT INTO deliveries VALUES (?, ?, ?, ?, ?, ?)", deliveries_data)
        conn.commit()
        config.logger.info("SQLite database generated successfully.")
    except sqlite3.Error as e:
        config.logger.error(f"Database error during generation: {e}")
        raise
    finally:
        if conn:
            conn.close()

#==================================================================================
# --- Weather Days Data Generation Functions ---
#==================================================================================

def generate_weather_data(num_days: int):
    """
    Generates a JSON file with simulated hourly weather data for a given period.\n
    This function creates a secondary data source used for data enrichment in the transform step.\n
    The weather conditions are randomly selected based on coefficients from the `domain` module.\n
    The JSON structure is a dictionary where each key represents a specific hour.\n
    Keys are formatted as 'YYYY-MM-DD-HH' for easy lookup.\n
    """
    config.logger.info(f"Generating JSON weather data at '{config.WEATHER_PATH}' for the last {num_days} days...")
    weather_data = {}
    start_date = datetime.now().replace(minute=0, second=0, microsecond=0) - timedelta(days=num_days)
    total_hours = num_days * 24
    for i in range(total_hours):
        current_time = start_date + timedelta(hours=i)
        time_key = current_time.strftime('%Y-%m-%d-%H')
        weather_data[time_key] = random.choice(list(domain.WEATHER_CONDITION_COEFFS.keys()))
    try:
        with open(config.WEATHER_PATH, 'w') as f:
            json.dump(weather_data, f, indent=4)
        config.logger.info("JSON weather data generated successfully.")
    except IOError as e:
        config.logger.error(f"Failed to write weather data to JSON file: {e}")
        raise