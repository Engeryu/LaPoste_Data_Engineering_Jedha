# supercourier_etl/core/extract.py
import polars as pl
import numpy as np
from faker import Faker
from datetime import timedelta
import os
from ..sources import readers

class Extractor:
    """Handles data extraction from generation or file sources."""

    READER_MAP = {
        ".csv": readers.CsvReader,
        ".json": readers.JsonReader,
        ".parquet": readers.ParquetReader,
        ".xlsx": readers.XlsxReader,
    }

    def __init__(self, config: dict):
        self.config = config
        self._fake = Faker()

    def extract_data(self) -> pl.DataFrame:
        """
        Extracts data based on the source type specified in the configuration.
        """
        source_config = self.config.get("source", {})
        source_type = source_config.get("type")

        if source_type == "generate":
            rows = source_config.get("rows", 1000)
            print(f"Generating {rows} synthetic records...")
            return self._generate_data(rows)
        
        elif source_type == "file":
            path = source_config.get("path")
            if not path:
                raise ValueError("Source type is 'file' but no path is specified.")
            
            print(f"Reading data from source file: {path}")
            file_extension = os.path.splitext(path)[1]
            reader_class = self.READER_MAP.get(file_extension)

            if not reader_class:
                raise ValueError(f"Unsupported file type: {file_extension}")
            
            reader = reader_class(path)
            return reader.read()
        
        else:
            raise ValueError(f"Unknown source type: {source_type}")

    def _generate_data(self, num_rows: int) -> pl.DataFrame:
        # ... (cette méthode ne change pas) ...
        package_types = ["Small", "Medium", "Large", "Extra Large", "Special"]
        delivery_zones = ["Urban", "Suburban", "Rural", "Industrial", "Shopping Center"]
        pickup_datetimes = [self._fake.date_time_between(start_date="-30d", end_date="now") for _ in range(num_rows)]
        delivery_timestamps = [pickup + timedelta(minutes=int(np.random.uniform(20, 360))) for pickup in pickup_datetimes]
        data = {
            "Delivery_ID": [f"SC{1000 + i}" for i in range(num_rows)],
            "Pickup_DateTime": pickup_datetimes,
            "Delivery_Timestamp": delivery_timestamps,
            "Package_Type": np.random.choice(package_types, num_rows, p=[0.4, 0.3, 0.15, 0.1, 0.05]),
            "Distance": np.random.uniform(1, 50, num_rows).round(2),
            "Delivery_Zone": np.random.choice(delivery_zones, num_rows, p=[0.35, 0.25, 0.2, 0.1, 0.1]),
        }
        return pl.from_dict(data)