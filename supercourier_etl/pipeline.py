# supercourier_etl/pipeline.py
import polars as pl
import time
from .core.extract import Extractor
from .core.transform import Transformer
from .core.load import Loader

class Pipeline:
    """
    Orchestrates the entire ETL process from extraction to loading.
    """

    def __init__(self, config: dict):
        """
        Initializes the pipeline with a given configuration.

        Args:
            config: A dictionary containing pipeline settings, such as source,
                    generation parameters, and output format.
        """
        self.config = config
        self.extractor = Extractor(config)
        self.transformer = Transformer(config)
        self.loader = Loader(config)

    def run(self):
        """
        Executes the ETL pipeline steps in order and measures execution time.
        """
        start_time = time.perf_counter()
        print("--- Starting ETL pipeline ---")

        extracted_data = self.extractor.extract_data()
        print(f"--- 1. Extraction complete. Found {len(extracted_data)} records. ---")

        transformed_data = self.transformer.transform_data(extracted_data)
        print("--- 2. Transformation complete. ---")

        self.loader.load_data(transformed_data)
        end_time = time.perf_counter()
        
        duration = end_time - start_time
        print("--- 3. Loading complete. ETL process finished successfully. ---")
        print(f"Total execution time: {duration:.2f} seconds.")