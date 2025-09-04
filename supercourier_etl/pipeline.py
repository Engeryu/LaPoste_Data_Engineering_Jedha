# supercourier_etl/pipeline.py
import polars as pl

from .core.extract import Extractor
from .core.transform import Transformer
from .core.load import Loader


class Pipeline:
    """Orchestrates the ETL process."""

    def __init__(self, config: dict):
        """
        Initializes the pipeline with a given configuration.

        Args:
            config (dict): A dictionary containing pipeline settings,
                           e.g., source path, generation parameters, output format.
        """
        self.config = config
        self.extractor = Extractor(config)
        self.transformer = Transformer(config)
        self.loader = Loader(config)

    def run(self):
        """Executes the ETL pipeline steps in order."""
        print("Starting ETL pipeline...")

        # 1. Extract
        extracted_data = self.extractor.extract_data()
        print(f"Extraction complete. Found {len(extracted_data)} records.")

        # 2. Transform
        transformed_data = self.transformer.transform_data(extracted_data)
        print("Transformation complete.")

        # 3. Load
        self.loader.load_data(transformed_data)
        print("Loading complete. ETL process finished successfully.")