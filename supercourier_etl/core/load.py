# supercourier_etl/core/load.py
import polars as pl
import json
from datetime import datetime
import os

class Loader:
    """Loads the transformed data and generates a metadata manifest."""

    def __init__(self, config: dict):
        self.config = config

    def load_data(self, df: pl.DataFrame):
        """
        Saves the DataFrame and generates a corresponding manifest file.
        """
        output_format = self.config.get("output", {}).get("format", "preview")
        
        if output_format == "preview":
            print("Loading data to destination (preview mode)...")
            print("Final DataFrame schema:")
            print(df)
        else:
            # We will implement file saving logic here later
            raise NotImplementedError("File saving is not yet implemented.")

        self._generate_manifest(df, output_format)

    def _generate_manifest(self, df: pl.DataFrame, output_format: str):
        """Generates a JSON manifest file with metadata about the run."""
        
        # We'll make the manifest path smarter later
        manifest_path = "output_manifest.json"
        
        print(f"  -> Generating metadata manifest at {manifest_path}...")
        
        manifest_data = {
            "polars_version": pl.__version__,
            "run_timestamp_utc": datetime.utcnow().isoformat(),
            "source_config": self.config.get("generate") or self.config.get("source"),
            "output_format": output_format,
            "dataset_shape": {
                "rows": df.height,
                "columns": df.width
            },
            "columns": df.columns,
        }
        
        with open(manifest_path, 'w') as f:
            json.dump(manifest_data, f, indent=4)