# supercourier_etl/core/load.py
import polars as pl

class Loader:
    """Loads the transformed data into a specified output format."""

    def __init__(self, config: dict):
        self.config = config

    def load_data(self, df: pl.DataFrame):
        """

        Saves the DataFrame to a file based on the output format
        specified in the configuration.

        Args:
            df (pl.DataFrame): The final, transformed DataFrame to be saved.
        """
        # Logic to select the correct writer will be added here
        print(f"Loading data to destination...")
        # For now, we just print the schema and a preview
        print("Final DataFrame schema:")
        print(df)
        print("\nPreview:")
        print(df.head(5))