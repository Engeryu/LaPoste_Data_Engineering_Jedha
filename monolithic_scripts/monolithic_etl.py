# monolithic_etl.py
"""
This script contains the entire SuperCourier ETL pipeline in a single, monolithic file.\n
It has been refactored to mirror the clean architecture of the main project,
consolidating all logic for configuration, file management, domain rules, data generation,
and the core ETL processes into one place for portability and simplicity.
"""

#==================================================================================
# --- Imports ---
#==================================================================================
import os
import time
import sqlite3
import json
import logging
import random
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

#==================================================================================
# --- Configuration & Logging ---
#==================================================================================
OUTPUT_DIR = 'output_files_mono'
ORIGINALS_DIR = os.path.join(OUTPUT_DIR, 'originals')
DB_PATH = os.path.join(ORIGINALS_DIR, 'supercourier_logistics.db')
WEATHER_PATH = os.path.join(ORIGINALS_DIR, 'weather_conditions.json')
OUTPUT_FILENAME_BASE = os.path.join(OUTPUT_DIR, 'deliveries_analysis')
LOG_PATH = os.path.join(OUTPUT_DIR, 'pipeline.log')

logger = logging.getLogger('supercourier_etl_mono')
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
if not logger.handlers:
    logger.addHandler(stream_handler)

def setup_file_logging():
    """Sets up the file handler for the application logger.
    
    This function ensures that any previous file handlers are closed and removed before adding a new one.
    This prevents duplicate log entries if the pipeline is run multiple times in the same session.
    
    """
    for handler in logger.handlers[:]:
        if isinstance(handler, logging.FileHandler):
            handler.close()
            logger.removeHandler(handler)
    file_handler = logging.FileHandler(LOG_PATH)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

MIN_ROW_DELIVERIES = 1000
MIN_NUM_WEATHER_DAYS = 90

#==================================================================================
# --- File Management Utilities ---
#==================================================================================
def archive_existing_file(file_path: str):
    """
    Archives an existing file by renaming it with an incremental suffix.
    
    If 'file.txt' exists, it is renamed to 'file_old.txt'.
    If 'file_old.txt' also exists, it is renamed to 'file_old_1.txt', and so on.
    This prevents accidental data loss during pipeline regeneration.
    
    """
    if not os.path.exists(file_path): return
    base, ext = os.path.splitext(file_path)
    old_path = f"{base}_old{ext}"
    if not os.path.exists(old_path):
        os.rename(file_path, old_path)
        logger.info(f"Archived '{os.path.basename(file_path)}' to '{os.path.basename(old_path)}'")
        return
    i = 1
    while True:
        numbered_old_path = f"{base}_old_{i}{ext}"
        if not os.path.exists(numbered_old_path):
            os.rename(file_path, numbered_old_path)
            logger.info(f"Archived '{os.path.basename(file_path)}' to '{os.path.basename(numbered_old_path)}'")
            break
        i += 1

def replace_main_file(file_path: str):
    """
    Deletes a specific file if it exists, without touching its archived versions.
    
    This is used when the user chooses to replace existing data but keep old archives.
    Logs an error if the file deletion fails.
    
    """
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.info(f"Replaced: '{os.path.basename(file_path)}' deleted.")
    except OSError as e:
        logger.error(f"Error deleting file {file_path}: {e}")

def delete_all_file_versions(file_path: str):
    """
    Performs a complete cleanup by deleting a file and all of its archived versions.
    
    It operates on a base file path (e.g., 'output/data.db').
    It will then delete all files in the same directory that match the pattern:
    - data.db
    - data_old.db
    - data_old_1.db
    
    This is useful for a full reset of the output directory.
    
    """
    try:
        dir_name = os.path.dirname(file_path)
        if not os.path.exists(dir_name): return
        base_name, ext = os.path.splitext(os.path.basename(file_path))
        for filename in os.listdir(dir_name):
            if filename.startswith(base_name) and filename.endswith(ext):
                path_to_delete = os.path.join(dir_name, filename)
                os.remove(path_to_delete)
                logger.info(f"Deleted file version: '{filename}'")
    except OSError as e:
        logger.error(f"Error deleting versions of {file_path}: {e}")

#==================================================================================
# --- Domain Logic & Business Rules ---
#==================================================================================
PACKAGE_TYPE_COEFFS = {'Small': 1.0, 'Medium': 1.2, 'Large': 1.5, 'Extra Large': 2.0, 'Special': 2.5}
DELIVERY_ZONE_COEFFS = {'Urban': 1.2, 'Suburban': 1.0, 'Rural': 1.3, 'Industrial': 0.9, 'Shopping Center': 1.4}
WEATHER_CONDITION_COEFFS = {'Sunny': 1.0, 'Cloudy': 1.05, 'Rainy': 1.2, 'Stormy': 1.5, 'Snowy': 1.8}
DAY_TYPE_COEFFS = {'Weekday': 1.1, 'Weekend': 0.9}
PEAK_HOUR_COEFFS = {'Morning Peak': 1.3, 'Evening Peak': 1.4, 'Off-Peak': 1.0, 'Night': 1.2}

def calculate_delivery_status(df: pd.DataFrame) -> pd.DataFrame:
    """Calculates the final delivery status for each record in the DataFrame.
    
    This is the core function where all business rules are applied using efficient, vectorized operations.
    It performs the following steps:
    - Determines the peak hour type for each delivery using `numpy.select` for performance.
    - Calculates an adjusted theoretical delivery time by applying all relevant coefficients.
    - Compares the actual delivery time against a calculated delay threshold.
    - Assigns a final status of 'On-time' or 'Delayed' to each delivery.
    
    Returns:
        The DataFrame with the new 'Status' and 'Peak_Hour_Type' columns.
        
    """
    conditions = [
        (df['Hour'] >= 7) & (df['Hour'] <= 9),
        (df['Hour'] >= 17) & (df['Hour'] <= 19),
        (df['Hour'] >= 20) | (df['Hour'] < 7)
    ]
    choices = ['Morning Peak', 'Evening Peak', 'Night']
    df['Peak_Hour_Type'] = np.select(conditions, choices, default='Off-Peak')
    base_theoretical_time = 30 + df['Distance'] * 0.8
    adjusted_theoretical_time = (
        base_theoretical_time *
        df['Package_Type'].map(PACKAGE_TYPE_COEFFS) *
        df['Delivery_Zone'].map(DELIVERY_ZONE_COEFFS) *
        df['Weather_Condition'].map(WEATHER_CONDITION_COEFFS).fillna(1.0) *
        df['Day_Type'].map(DAY_TYPE_COEFFS) *
        df['Peak_Hour_Type'].map(PEAK_HOUR_COEFFS)
    )
    delay_threshold = adjusted_theoretical_time * 1.2
    df['Status'] = np.where(df['Actual_Delivery_Time_Minutes'] > delay_threshold, 'Delayed', 'On-time')
    return df

#==================================================================================
# --- Data Generation Functions ---
#==================================================================================
def generate_sqlite_database(rows_deliveries: int):
    """
    Generates a SQLite database with simulated delivery tracking data.
    
    This function creates the primary data source for the ETL pipeline.
    It populates a 'deliveries' table with a specified number of records.
    The table includes the following fields:
    - Delivery_ID
    - Pickup_DateTime
    - Package_Type
    - Distance
    - Delivery_Zone
    - Actual_Delivery_Time_Minutes
    
    Data values are randomized but weighted using coefficients from the `domain` module to ensure realism.
    
    """
    logger.info(f"Generating SQLite database at '{DB_PATH}' with {rows_deliveries} records...")
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS deliveries")
        cursor.execute("""
            CREATE TABLE deliveries (
                Delivery_ID TEXT PRIMARY KEY, Pickup_DateTime TEXT, Package_Type TEXT,
                Distance REAL, Delivery_Zone TEXT, Actual_Delivery_Time_Minutes REAL
            )""")
        deliveries_data = []
        start_date = datetime.now() - timedelta(days=MIN_NUM_WEATHER_DAYS)
        for i in range(rows_deliveries):
            pickup_time = start_date + timedelta(seconds=random.randint(0, MIN_NUM_WEATHER_DAYS * 86400))
            distance = round(random.uniform(1.0, 75.0), 2)
            base_time = 20 + distance * 1.5
            random_delay = random.uniform(-10, 45)
            actual_time = round(max(15, base_time + random_delay), 2)
            deliveries_data.append((
                f"SC-DEL-{i+1:05d}",
                pickup_time.strftime('%Y-%m-%d %H:%M:%S'),
                random.choice(list(PACKAGE_TYPE_COEFFS.keys())),
                distance,
                random.choice(list(DELIVERY_ZONE_COEFFS.keys())),
                actual_time
            ))
        cursor.executemany("INSERT INTO deliveries VALUES (?, ?, ?, ?, ?, ?)", deliveries_data)
        conn.commit()
        logger.info("SQLite database generated successfully.")
    except sqlite3.Error as e:
        logger.error(f"Database error during generation: {e}")
        raise
    finally:
        if conn:
            conn.close()

def generate_weather_data(num_days: int):
    """
    Generates a JSON file with simulated hourly weather data for a given period.
    
    This function creates a secondary data source used for data enrichment in the transform step.
    The weather conditions are randomly selected based on coefficients from the `domain` module.
    The JSON structure is a dictionary where each key represents a specific hour.
    
    Keys are formatted as 'YYYY-MM-DD-HH' for easy lookup.
    
    """
    logger.info(f"Generating JSON weather data at '{WEATHER_PATH}' for the last {num_days} days...")
    weather_data = {}
    start_date = datetime.now().replace(minute=0, second=0, microsecond=0) - timedelta(days=num_days)
    total_hours = num_days * 24
    for i in range(total_hours):
        current_time = start_date + timedelta(hours=i)
        time_key = current_time.strftime('%Y-%m-%d-%H')
        weather_data[time_key] = random.choice(list(WEATHER_CONDITION_COEFFS.keys()))
    try:
        with open(WEATHER_PATH, 'w') as f:
            json.dump(weather_data, f, indent=4)
        logger.info("JSON weather data generated successfully.")
    except IOError as e:
        logger.error(f"Failed to write weather data to JSON file: {e}")
        raise

#==================================================================================
# --- ETL Pipeline Functions (Extract, Transform, Load) ---
#==================================================================================
def extract_data_from_sqlite() -> pd.DataFrame:
    """Extracts the complete 'deliveries' table from the source SQLite database.
    
    This function represents the 'Extract' step of the ETL process.
    It connects to the database path defined in the configuration and loads the data into a pandas DataFrame.
    
    Raises:
        FileNotFoundError: If the database file does not exist.
    """
    logger.info(f"Extracting data from '{DB_PATH}'...")
    if not os.path.exists(DB_PATH):
        logger.error(f"Database file not found at '{DB_PATH}'.")
        raise FileNotFoundError(f"Database file not found at '{DB_PATH}'.")
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql_query("SELECT * FROM deliveries", conn)
        logger.info(f"Successfully extracted {len(df)} records from the database.")
        return df
    except sqlite3.Error as e:
        logger.error(f"Failed to extract data from SQLite: {e}")
        raise
    finally:
        if conn:
            conn.close()

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
        with open(WEATHER_PATH, 'r') as f:
            weather_data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.error(f"Could not load or parse weather data: {e}")
        raise
    df['Weather_Key'] = df['Pickup_DateTime'].dt.strftime('%Y-%m-%d-%H')
    df['Weather_Condition'] = df['Weather_Key'].map(weather_data).fillna('Unknown')
    if 'Unknown' in df['Weather_Condition'].unique():
        logger.warning("Some records could not be matched with weather data.")
    return df

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """Orchestrates the full data transformation pipeline.
    
    This function applies a series of enrichment and business logic steps in a specific order:
    1. Adds time-based features using `_add_datetime_features`.
    2. Enriches the data with weather conditions using `_enrich_with_weather`.
    3. Applies core business rules to calculate delivery status via `calculate_delivery_status`.
    4. Selects and renames the final columns for the output dataset.
    
    Returns:
        A clean, analysis-ready DataFrame.
        
    """
    logger.info("Starting data transformation pipeline...")
    df_transformed = _add_datetime_features(df)
    df_transformed = _enrich_with_weather(df_transformed)
    df_final_logic = calculate_delivery_status(df_transformed)

    # Convert the float minutes to a MM.SS string format
    minutes = df_final_logic['Actual_Delivery_Time_Minutes'].astype(int)
    seconds = ((df_final_logic['Actual_Delivery_Time_Minutes'] - minutes) * 60).astype(int)
    df_final_logic['Actual_Delivery_Time'] = minutes.astype(str).str.zfill(2) + '.' + seconds.astype(str).str.zfill(2)

    final_cols = [
        'Delivery_ID', 'Pickup_DateTime', 'Weekday', 'Hour', 'Package_Type', 'Distance',
        'Delivery_Zone', 'Weather_Condition', 'Actual_Delivery_Time', 'Status'
    ]
    final_df = df_final_logic[final_cols].copy()
    
    logger.info("Data transformation complete.")
    return final_df

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
    output_path = f"{OUTPUT_FILENAME_BASE}.{output_format}"
    logger.info(f"Loading final data to '{output_path}'...")
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
            logger.error(f"Unsupported format '{output_format}'.")
            return
        logger.info(f"Successfully saved the final dataset to '{output_path}'.")
    except Exception as e:
        logger.error(f"Failed to write data to {output_format} format: {e}")
        raise

#==================================================================================
# --- Main Pipeline Orchestrator ---
#==================================================================================
class SuperCourierPipeline:
    """
    Encapsulates and orchestrates the entire ETL pipeline from start to finish.
    
    This class manages user interactions, file system operations, and the sequential execution of the ETL steps.
    It delegates specific tasks to the appropriate modules for data generation, transformation, and loading.
    
    """
    def __init__(self):
        """
        Initializes the pipeline with default data generation parameters.
        
        Sets the number of delivery records and weather days to their default values from the config module.
        
        """
        self.rows_deliveries = MIN_ROW_DELIVERIES
        self.num_weather_days = MIN_NUM_WEATHER_DAYS

    def _get_user_parameters(self):
        """
        Prompts the user to configure the data generation parameters via the command line.
        """
        print("\n--- Configure Data Generation ---")
        while True:
            try:
                val = input(f"Enter number of deliveries to generate (default: {self.rows_deliveries}, min: 1000): ")
                if val == "": break
                num = int(val)
                if num < 1000:
                    print("Error: Value must be at least 1000.")
                    continue
                self.rows_deliveries = num
                break
            except ValueError:
                print("Invalid input. Please enter a whole number.")
        while True:
            try:
                val = input(f"Enter number of weather days to generate (default: {self.num_weather_days}, min: 90): ")
                if val == "": break
                num = int(val)
                if num < 90:
                    print("Error: Value must be at least 90.")
                    continue
                self.num_weather_days = num
                break
            except ValueError:
                print("Invalid input. Please enter a whole number.")

    def _handle_existing_files(self):
        """
        Manages pre-existing data and log files to ensure a clean run.
        
        Prompts the user to choose an action for existing files:
        - Archive current files.
        - Replace current files (while keeping archives).
        - Delete all versions of all files for a complete cleanup.
        
        Delegates the chosen file operation to the `file_manager` module.
        """
        if not os.path.exists(DB_PATH) and not os.path.exists(WEATHER_PATH):
            return
        print("\n--- File Management ---")
        user_choice = ''
        while user_choice not in ['1', '2', '3']:
            user_choice = input(
                "Existing data files found. What would you like to do?\n"
                " [1] Archive current files\n"
                " [2] Replace current files (keeps archives)\n"
                " [3] Delete ALL files (current and archived)\n"
                "Your choice: "
            )
        for handler in logger.handlers[:]:
            if isinstance(handler, logging.FileHandler):
                handler.close()
                logger.removeHandler(handler)
        source_files = [DB_PATH, WEATHER_PATH]
        analysis_base_name = os.path.basename(OUTPUT_FILENAME_BASE)
        if user_choice == '1':
            print(" -> Archiving current files...")
            archive_existing_file(LOG_PATH)
            for f in source_files: archive_existing_file(f)
            for filename in os.listdir(OUTPUT_DIR):
                if filename.startswith(analysis_base_name):
                    archive_existing_file(os.path.join(OUTPUT_DIR, filename))
        elif user_choice == '2':
            print(" -> Replacing current files...")
            replace_main_file(LOG_PATH)
            for f in source_files: replace_main_file(f)
            for filename in os.listdir(OUTPUT_DIR):
                if filename.startswith(analysis_base_name) and '_old' not in filename:
                    replace_main_file(os.path.join(OUTPUT_DIR, filename))
        elif user_choice == '3':
            print(" -> Deleting all file versions...")
            all_formats = ['csv', 'parquet', 'json', 'db', 'xlsx']
            delete_all_file_versions(LOG_PATH)
            delete_all_file_versions(DB_PATH)
            delete_all_file_versions(WEATHER_PATH)
            for fmt in all_formats:
                output_path_to_delete = f"{OUTPUT_FILENAME_BASE}.{fmt}"
                delete_all_file_versions(output_path_to_delete)

    def _get_output_format(self) -> str:
        """
        Prompts the user to select the desired output format for the final dataset.
        
        Provides a menu of supported formats, including individual options and bulk selections.
        Returns the user's selection as a string for the loading step.
        
        """
        print("\n--- Choose Output Format ---")
        formats = {'1': 'csv', '2': 'parquet', '3': 'json', '4': 'db', '5': 'xlsx', '6': 'No xlsx', '7': 'all'}
        while True:
            choice = input(
                "In which format would you like to save the final data?\n"
                " [1] CSV\n [2] Parquet\n [3] JSON\n [4] SQLite DB\n [5] Excel (.xlsx)\n [6] No xlsx\n [7] All of the above\n"
                "Your choice: "
            )
            if choice in formats: return formats[choice]
            print("Invalid choice, please try again.")

    def run(self):
        """
        Executes the full ETL pipeline in a sequential and orchestrated manner.
        
        This method acts as the main conductor, calling other methods and modules in the correct order:
        1. Gathers user parameters for data generation.
        2. Manages existing files based on user choice.
        3. Sets up file-based logging for the current run.
        4. Triggers the generation of source data.
        5. Executes the Extract, Transform, and Load steps via the `etl_pipeline` module.
        6. Measures and logs the total execution time.
        
        """
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        os.makedirs(ORIGINALS_DIR, exist_ok=True)
        self._get_user_parameters()
        self._handle_existing_files()
        chosen_format = self._get_output_format()
        setup_file_logging()
        print("\n--- SuperCourier ETL Pipeline Initializing ---")
        start_time = time.perf_counter()
        print("\n[1/4] Generating new data files...")
        generate_sqlite_database(self.rows_deliveries)
        generate_weather_data(self.num_weather_days)
        print(" -> Source files are ready.")
        print("\n[2/4] Extracting data from SQLite database...")
        raw_df = extract_data_from_sqlite()
        print(f" -> Extraction complete. {len(raw_df)} rows loaded.")
        print("\n[3/4] Transforming and enriching data...")
        transformed_df = transform_data(raw_df)
        print(" -> Transformation complete.")
        print(f"\n[4/4] Loading final data...")
        if chosen_format == 'all':
            all_formats = ['csv', 'parquet', 'json', 'db', 'xlsx']
            print(f" -> Saving to all formats: {', '.join(all_formats)}")
            for fmt in all_formats: load_data(transformed_df, fmt)
        elif chosen_format == 'No xlsx':
            formats_without_xlsx = ['csv', 'parquet', 'json', 'db']
            print(f" -> Saving to all formats except Excel: {', '.join(formats_without_xlsx)}")
            for fmt in formats_without_xlsx: load_data(transformed_df, fmt)
        else:
            print(f" -> Saving to .{chosen_format} format...")
            load_data(transformed_df, chosen_format)
        print(f" -> Load complete. Check the '{OUTPUT_DIR}' directory.\n")
        end_time = time.perf_counter()
        duration = end_time - start_time
        logger.info(f"ETL Pipeline completed successfully in {duration:.2f} seconds.")
        print(f"BENCHMARK_TIME:{duration:.2f}")
        print("--- Pipeline Finished ---")

#==================================================================================
# --- Main Entry Point ---
#==================================================================================
def main():
    """
    Main entry point for the SuperCourier ETL application.
    
    This function creates an instance of the `SuperCourierPipeline` and executes its `run` method.
    It includes top-level error handling to catch and log any critical exceptions during the pipeline's execution.
    """
    try:
        pipeline = SuperCourierPipeline()
        pipeline.run()
    except (FileNotFoundError, sqlite3.Error, IOError, Exception) as e:
        logger.critical(f"A critical error stopped the pipeline: {e}", exc_info=True)
        print(f"\n--- PIPELINE FAILED ---")
        print(f"An unexpected error occurred. Check '{LOG_PATH}' for details if it was created.")

if __name__ == "__main__":
    main()