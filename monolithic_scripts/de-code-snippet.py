# SuperCourier - Mini ETL Pipeline
# Starter code for the Data Engineering mini-challenge

import sqlite3
import pandas as pd
import numpy as np
import json
import logging
from datetime import datetime, timedelta
import random
import os

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('supercourier_mini_etl')

# Constants
DB_PATH = 'supercourier_mini.db'
WEATHER_PATH = 'weather_data.json'
OUTPUT_PATH = 'deliveries.csv'

# 1. FUNCTION TO GENERATE SQLITE DATABASE (you can modify as needed)
def create_sqlite_database():
    """
    Creates a simple SQLite database with a deliveries table
    """
    logger.info("Creating SQLite database...")
    
    # Remove database if it already exists
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    
    # Connect to database
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Create deliveries table
    cursor.execute('''
    CREATE TABLE deliveries (
        delivery_id INTEGER PRIMARY KEY,
        pickup_datetime TEXT,
        package_type TEXT,
        delivery_zone TEXT,
        recipient_id INTEGER
    )
    ''')
    
    # Available package types and delivery zones
    package_types = ['Small', 'Medium', 'Large', 'X-Large', 'Special']
    delivery_zones = ['Urban', 'Suburban', 'Rural', 'Industrial', 'Shopping Center']
    
    # Generate 1000 random deliveries
    deliveries = []
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=90)  # 3 months
    
    for i in range(1, 1001):
        # Random date within last 3 months
        timestamp = start_date + timedelta(
            seconds=random.randint(0, int((end_date - start_date).total_seconds()))
        )
        
        # Random selection of package type and zone
        package_type = random.choices(
            package_types, 
            weights=[25, 30, 20, 15, 10]  # Relative probabilities
        )[0]
        
        delivery_zone = random.choice(delivery_zones)
        
        # Add to list
        deliveries.append((
            i,  # delivery_id
            timestamp.strftime('%Y-%m-%d %H:%M:%S'),  # pickup_datetime
            package_type,
            delivery_zone,
            random.randint(1, 100)  # fictional recipient_id
        ))
    
    # Insert data
    cursor.executemany(
        'INSERT INTO deliveries VALUES (?, ?, ?, ?, ?)',
        deliveries
    )
    
    # Commit and close
    conn.commit()
    conn.close()
    
    logger.info(f"Database created with {len(deliveries)} deliveries")
    return True

# 2. FUNCTION TO GENERATE WEATHER DATA
def generate_weather_data():
    """
    Generates fictional weather data for the last 3 months
    """
    logger.info("Generating weather data...")
    
    conditions = ['Sunny', 'Cloudy', 'Rainy', 'Windy', 'Snowy', 'Foggy']
    weights = [30, 25, 20, 15, 5, 5]  # Relative probabilities
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=90)
    
    weather_data = {}
    
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')
        weather_data[date_str] = {}
        
        # For each day, generate weather for each hour
        for hour in range(24):
            # More continuity in conditions
            if hour > 0 and random.random() < 0.7:
                # 70% chance of keeping same condition as previous hour
                condition = weather_data[date_str].get(str(hour-1), 
                                                      random.choices(conditions, weights=weights)[0])
            else:
                condition = random.choices(conditions, weights=weights)[0]
            
            weather_data[date_str][str(hour)] = condition
        
        current_date += timedelta(days=1)
    
    # Save as JSON
    with open(WEATHER_PATH, 'w', encoding='utf-8') as f:
        json.dump(weather_data, f, ensure_ascii=False, indent=2)
    
    logger.info(f"Weather data generated for period {start_date.strftime('%Y-%m-%d')} - {end_date.strftime('%Y-%m-%d')}")
    return weather_data

# 3. EXTRACTION FUNCTIONS
def extract_sqlite_data():
    """
    Extracts delivery data from SQLite database
    """
    logger.info("Extracting data from SQLite database...")
    
    conn = sqlite3.connect(DB_PATH)
    query = "SELECT * FROM deliveries"
    df = pd.read_sql(query, conn)
    conn.close()
    
    logger.info(f"Extraction complete: {len(df)} records")
    return df

def load_weather_data():
    """
    Loads weather data from JSON file
    """
    logger.info("Loading weather data...")
    
    with open(WEATHER_PATH, 'r', encoding='utf-8') as f:
        weather_data = json.load(f)
    
    logger.info(f"Weather data loaded for {len(weather_data)} days")
    return weather_data

# 4. TRANSFORMATION FUNCTIONS
def enrich_with_weather(df, weather_data):
    """
    Enriches the DataFrame with weather conditions and time-related features
    """
    logger.info("Enriching with weather data...")
    
    # Convert date column to datetime
    df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
    
    # Function to get weather for a given timestamp
    def get_weather(timestamp):
        date_str = timestamp.strftime('%Y-%m-%d')
        hour_str = str(timestamp.hour)
        
        try:
            return weather_data[date_str][hour_str]
        except KeyError:
            return None
    
    # Apply function to each row
    df['WeatherCondition'] = df['pickup_datetime'].apply(get_weather)
    
    # Add Weekday and Hour columns
    df['Weekday'] = df['pickup_datetime'].dt.day_name()
    df['Hour'] = df['pickup_datetime'].dt.hour
    
    return df

def transform_data(df_deliveries, weather_data):
    """
    Main data transformation function
    """
    logger.info("Transforming data...")
    
    # 1. Enrich with weather data and time features
    df = enrich_with_weather(df_deliveries, weather_data)
    
    # 2. Handle missing values
    df['WeatherCondition'] = df['WeatherCondition'].fillna('No Data')
    
    # 3. Simulate tracking logs: generate Distance and Actual_Delivery_Time
    df['Distance'] = np.random.uniform(5, 50, len(df)).round(2)  # Random distance between 5 and 50 km
    
    # Base theoretical time: 30 + distance * 0.8
    df['Theoretical_Time_Base'] = 30 + df['Distance'] * 0.8
    
    # 4. Calculate delivery times and status
    
    # Define adjustment factors
    package_factors = {
        'Small': 1, 'Medium': 1.2, 'Large': 1.5, 'X-Large': 2, 'Special': 2.5
    }
    zone_factors = {
        'Urban': 1.2, 'Suburban': 1, 'Rural': 1.3, 'Industrial': 0.9, 'Shopping Center': 1.4
    }
    weather_factors = {
        'Sunny': 1, 'Cloudy': 1.05, 'Rainy': 1.2, 'Windy': 1.5, 'Snowy': 1.8, 'Foggy': 1.2, 'No Data': 1.1
    }
    
    # Calculate adjusted theoretical time
    df['Theoretical_Time_Adjusted'] = df['Theoretical_Time_Base'] * \
                                       df['package_type'].map(package_factors) * \
                                       df['delivery_zone'].map(zone_factors) * \
                                       df['WeatherCondition'].map(weather_factors)
    
    # Apply time of day adjustments
    df.loc[(df['Hour'] >= 7) & (df['Hour'] <= 9), 'Theoretical_Time_Adjusted'] *= 1.3  # Morning peak
    df.loc[(df['Hour'] >= 16) & (df['Hour'] <= 18), 'Theoretical_Time_Adjusted'] *= 1.4  # Evening peak
    
    # Apply day of week adjustments
    df.loc[df['Weekday'].isin(['Monday', 'Friday']), 'Theoretical_Time_Adjusted'] *= 1.2
    df.loc[df['Weekday'].isin(['Saturday', 'Sunday']), 'Theoretical_Time_Adjusted'] *= 0.9
    
    # Calculate delay threshold
    df['Delay_Threshold'] = df['Theoretical_Time_Adjusted'] * 1.2
    
    # Simulate Actual_Delivery_Time based on the theoretical time
    df['Actual_Delivery_Time'] = df.apply(lambda row: random.uniform(
        row['Theoretical_Time_Adjusted'] * 0.8, 
        row['Theoretical_Time_Adjusted'] * 1.5
    ), axis=1)
    
    # Determine delivery status
    df['Status'] = np.where(
        df['Actual_Delivery_Time'] > df['Delay_Threshold'], 
        'Delayed', 
        'On-time'
    )
    
    # Final cleanup of columns to match the expected structure
    df = df.rename(columns={'delivery_id': 'Delivery_ID', 'package_type': 'Package_Type',
                           'delivery_zone': 'Delivery_Zone'})
    
    # Drop intermediate columns
    df = df.drop(columns=['recipient_id', 'Theoretical_Time_Base', 'Theoretical_Time_Adjusted', 'Delay_Threshold'])
    
    logger.info("Transformation complete.")
    
    return df  # Return transformed DataFrame

# 5. LOADING FUNCTION
def save_results(df):
    """
    Saves the final DataFrame to CSV
    """
    logger.info("Saving results...")
    
    # Data validation: check for missing values
    if df.isnull().values.any():
        logger.warning("Missing values found in the final DataFrame. Handling...")
        df = df.fillna('Unknown')
    
    # Save to CSV
    df.to_csv(OUTPUT_PATH, index=False)
    
    logger.info(f"Results saved to {OUTPUT_PATH}")
    return True

# MAIN FUNCTION
def run_pipeline():
    """
    Runs the ETL pipeline end-to-end
    """
    try:
        logger.info("Starting SuperCourier ETL pipeline")
        
        # Step 1: Generate data sources
        create_sqlite_database()
        weather_data = generate_weather_data()
        
        # Step 2: Extraction
        df_deliveries = extract_sqlite_data()
        
        # Step 3: Transformation
        df_transformed = transform_data(df_deliveries, weather_data)
        
        # Step 4: Loading
        save_results(df_transformed)
        
        logger.info("ETL pipeline completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error during pipeline execution: {str(e)}")
        return False

# Main entry point
if __name__ == "__main__":
    run_pipeline()