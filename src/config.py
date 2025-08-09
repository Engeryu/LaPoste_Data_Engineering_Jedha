# supercourier_etl/config.py
"""
Central configuration module for the SuperCourier ETL pipeline.\n
This file defines all shared constants and settings for the application.\n
It includes file paths, logging configuration, and data generation parameters.\n
"""

# Imports of the necessary libraries
import os
import logging

# --- File Paths ---
"""Defines the directory structure and file paths used throughout the application.\n
- OUTPUT_DIR: The main directory for all generated output files.\n
- ORIGINALS_DIR: A subdirectory for storing the raw, generated source data.\n
- DB_PATH: The full path to the SQLite database file.\n
- WEATHER_PATH: The full path to the JSON file for weather data.\n
- OUTPUT_FILENAME_BASE: The base name for all analysis output files, without the extension.\n
- LOG_PATH: The full path to the application's log file.\n
"""
OUTPUT_DIR = 'output_files'
ORIGINALS_DIR = os.path.join(OUTPUT_DIR, 'originals')
DB_PATH = os.path.join(ORIGINALS_DIR, 'supercourier_logistics.db')
WEATHER_PATH = os.path.join(ORIGINALS_DIR, 'weather_conditions.json')
OUTPUT_FILENAME_BASE = os.path.join(OUTPUT_DIR, 'deliveries_analysis')
LOG_PATH = os.path.join(OUTPUT_DIR, 'pipeline.log')

# --- Logging Configuration ---
"""Configures the application-wide logger for both console and file output.\n
This setup ensures that all events are captured for monitoring and debugging purposes.\n
The logger is initialized once to prevent duplicate handlers.\n
"""
logger = logging.getLogger('supercourier_etl')
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
if not logger.handlers: # Avoid adding duplicate handlers
    logger.addHandler(stream_handler)

def setup_file_logging():
    """Sets up the file handler for the application logger.\n
    This function ensures that any previous file handlers are closed and removed before adding a new one.\n
    This prevents duplicate log entries if the pipeline is run multiple times in the same session.\n
    """
    # Remove existing file handler before adding a new one
    for handler in logger.handlers[:]:
        if isinstance(handler, logging.FileHandler):
            handler.close()
            logger.removeHandler(handler)
            
    file_handler = logging.FileHandler(LOG_PATH)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

# --- Data Generation Parameters ---
"""Defines the parameters for synthetic data generation.\n
These constants control the size and scope of the generated datasets.\n
- MIN_ROW_DELIVERIES: The default and minimum number of delivery records to generate.\n
- MIN_NUM_WEATHER_DAYS: The default and minimum number of days for which to generate weather data.\n
"""
MIN_ROW_DELIVERIES = 1000
MIN_NUM_WEATHER_DAYS = 90