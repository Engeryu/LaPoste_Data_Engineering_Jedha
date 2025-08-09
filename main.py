# main.py
"""
Main entry point for the SuperCourier ETL application.\n
This script initializes and runs the main ETL pipeline, handling user interactions
for parameter configuration and file management.\n
"""

# Imports of the necessary libraries
import os
import time
import logging
import sqlite3
# Imports from the application package (All local modules)
from src import config, data_generators, etl_pipeline, file_manager

class SuperCourierPipeline:
    """
    Encapsulates and orchestrates the entire ETL pipeline from start to finish.\n
    This class manages user interactions, file system operations, and the sequential execution of the ETL steps.\n
    It delegates specific tasks to the appropriate modules for data generation, transformation, and loading.\n
    """

    def __init__(self):
        """
        Initializes the pipeline with default data generation parameters.\n
        Sets the number of delivery records and weather days to their default values from the config module.\n
        """
        self.rows_deliveries = config.MIN_ROW_DELIVERIES
        self.num_weather_days = config.MIN_NUM_WEATHER_DAYS

    def _get_user_parameters(self):
        """
        Prompts the user to configure the data generation parameters via the command line.\n
        Allows customization of the number of delivery records and weather days.\n
        Ensures that user inputs are valid integers and meet the minimum requirements defined in the config.\n
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
        Manages pre-existing data and log files to ensure a clean run.\n
        Prompts the user to choose an action for existing files:\n
        - Archive current files.\n
        - Replace current files (while keeping archives).\n
        - Delete all versions of all files for a complete cleanup.\n
        Delegates the chosen file operation to the `file_manager` module.\n
        """
        if not os.path.exists(config.DB_PATH) and not os.path.exists(config.WEATHER_PATH):
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
        
        # Release the log handler before performing file operations.
        for handler in config.logger.handlers[:]:
            if isinstance(handler, logging.FileHandler):
                handler.close()
                config.logger.removeHandler(handler)

        # Handle all relevant files, including source data, analysis outputs, and logs.
        source_files = [config.DB_PATH, config.WEATHER_PATH]
        analysis_base_name = os.path.basename(config.OUTPUT_FILENAME_BASE)

        if user_choice == '1':
            print(" -> Archiving current files...")
            file_manager.archive_existing_file(config.LOG_PATH)
            for f in source_files:
                file_manager.archive_existing_file(f)
            # Find and archive all corresponding analysis files
            for filename in os.listdir(config.OUTPUT_DIR):
                if filename.startswith(analysis_base_name):
                    file_manager.archive_existing_file(os.path.join(config.OUTPUT_DIR, filename))
        
        elif user_choice == '2':
            print(" -> Replacing current files...")
            file_manager.replace_main_file(config.LOG_PATH)
            for f in source_files:
                file_manager.replace_main_file(f)
            # Find and replace all corresponding non-archived analysis files
            for filename in os.listdir(config.OUTPUT_DIR):
                if filename.startswith(analysis_base_name) and '_old' not in filename:
                    file_manager.replace_main_file(os.path.join(config.OUTPUT_DIR, filename))

        elif user_choice == '3':
            print(" -> Deleting all file versions...")
            all_formats = ['csv', 'parquet', 'json', 'db', 'xlsx']
            file_manager.delete_all_file_versions(config.LOG_PATH)
            file_manager.delete_all_file_versions(config.DB_PATH)
            file_manager.delete_all_file_versions(config.WEATHER_PATH)
            for fmt in all_formats:
                output_path_to_delete = f"{config.OUTPUT_FILENAME_BASE}.{fmt}"
                file_manager.delete_all_file_versions(output_path_to_delete)
    
    def _get_output_format(self) -> str:
        """
        Prompts the user to select the desired output format for the final dataset.\n
        Provides a menu of supported formats, including individual options and bulk selections.\n
        Returns the user's selection as a string for the loading step.\n
        """
        print("\n--- Choose Output Format ---")
        formats = {'1': 'csv', '2': 'parquet', '3': 'json', '4': 'db', '5': 'xlsx', '6': 'No xlsx', '7': 'all'}
        while True:
            choice = input(
                "In which format would you like to save the final data?\n"
                " [1] CSV\n [2] Parquet\n [3] JSON\n [4] SQLite DB\n [5] Excel (.xlsx)\n [6] No xlsx\n [7] All of the above\n"
                "Your choice: "
            )
            if choice in formats:
                return formats[choice]
            print("Invalid choice, please try again.")

    def run(self):
        """
        Executes the full ETL pipeline in a sequential and orchestrated manner.\n
        This method acts as the main conductor, calling other methods and modules in the correct order:\n
        1. Gathers user parameters for data generation.\n
        2. Manages existing files based on user choice.\n
        3. Sets up file-based logging for the current run.\n
        4. Triggers the generation of source data.\n
        5. Executes the Extract, Transform, and Load steps via the `etl_pipeline` module.\n
        6. Measures and logs the total execution time.\n
        """
        os.makedirs(config.OUTPUT_DIR, exist_ok=True)
        os.makedirs(config.ORIGINALS_DIR, exist_ok=True)

        self._get_user_parameters()
        self._handle_existing_files()
        chosen_format = self._get_output_format()
        
        config.setup_file_logging()

        print("\n--- SuperCourier ETL Pipeline Initializing ---")
        start_time = time.perf_counter()
        
        print("\n[1/4] Generating new data files...")
        data_generators.generate_sqlite_database(self.rows_deliveries)
        data_generators.generate_weather_data(self.num_weather_days)
        print(" -> Source files are ready.")

        print("\n[2/4] Extracting data from SQLite database...")
        raw_df = etl_pipeline.extract_data_from_sqlite()
        print(f" -> Extraction complete. {len(raw_df)} rows loaded.")

        print("\n[3/4] Transforming and enriching data...")
        transformed_df = etl_pipeline.transform_data(raw_df)
        print(" -> Transformation complete.")
        
        print(f"\n[4/4] Loading final data...")
        if chosen_format == 'all':
            all_formats = ['csv', 'parquet', 'json', 'db', 'xlsx']
            print(f" -> Saving to all formats: {', '.join(all_formats)}")
            for fmt in all_formats:
                etl_pipeline.load_data(transformed_df, fmt)
        elif chosen_format == 'No xlsx':
            formats_without_xlsx = ['csv', 'parquet', 'json', 'db']
            print(f" -> Saving to all formats except Excel: {', '.join(formats_without_xlsx)}")
            for fmt in formats_without_xlsx:
                etl_pipeline.load_data(transformed_df, fmt)
        else:
            print(f" -> Saving to .{chosen_format} format...")
            etl_pipeline.load_data(transformed_df, chosen_format)
        
        print(f" -> Load complete. Check the '{config.OUTPUT_DIR}' directory.\n")
        
        end_time = time.perf_counter()
        duration = end_time - start_time
        config.logger.info(f"ETL Pipeline completed successfully in {duration:.2f} seconds.")
        print(f"BENCHMARK_TIME:{duration:.2f}")
        print("--- Pipeline Finished ---")

def main():
    """
    Main entry point for the SuperCourier ETL application.\n
    This function creates an instance of the `SuperCourierPipeline` and executes its `run` method.\n
    It includes top-level error handling to catch and log any critical exceptions during the pipeline's execution.\n
    """
    try:
        pipeline = SuperCourierPipeline()
        pipeline.run()
    except (FileNotFoundError, sqlite3.Error, IOError, Exception) as e:
        logging.getLogger('supercourier_etl').critical(f"A critical error stopped the pipeline: {e}", exc_info=True)
        print(f"\n--- PIPELINE FAILED ---")
        print(f"An unexpected error occurred. Check '{config.LOG_PATH}' for details if it was created.")

if __name__ == "__main__":
    main()