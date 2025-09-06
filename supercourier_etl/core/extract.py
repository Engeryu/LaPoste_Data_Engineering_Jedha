# supercourier_etl/core/extract.py
"""
    
"""
import os
from datetime import timedelta
import polars as pl
import numpy as np
from faker import Faker

from ..sources import readers

class Extractor:
    """Handles data extraction from generation or file sources."""

    READER_MAP = {
        ".csv": readers.CsvReader,
        ".json": readers.JsonReader,
        ".parquet": readers.ParquetReader,
        ".db": readers.DatabaseReader,
        ".xlsx": readers.XlsxReader
    }

    def __init__(self, config: dict):
        """
        Initializes the Extractor with the pipeline configuration.

        Args:
            config: The pipeline configuration dictionary.
        """
        self.config = config
        self._fake = Faker()

    def extract_data(self, progress=None, task_id=None) -> pl.DataFrame:
        """
        Extracts data based on the source type specified in the configuration.

        This method acts as a dispatcher, calling the appropriate helper method
        (generation or file reading) based on the config.

        Args:
            progress: A rich.progress object for updating the progress bar.
            task_id: The ID of the progress bar task to update.

        Returns:
            A Polars DataFrame containing the source data.
        """
        source_config = self.config.get("source", {})
        source_type = source_config.get("type")

        df: pl.DataFrame

        if source_type == "generate":
            rows = source_config.get("rows", 1000)
            df = self._generate_data(rows, progress, task_id)

        elif source_type == "file":
            path = source_config.get("path")
            if not path or not os.path.exists(path):
                raise FileNotFoundError(f"Source file not found at path: {path}")

            file_extension = os.path.splitext(path)[1]
            reader_class = self.READER_MAP.get(file_extension)

            if not reader_class:
                raise ValueError(f"Unsupported file type: {file_extension}")

            reader = reader_class(path)
            df = reader.read()

            if progress and task_id is not None:
                progress.update(task_id, completed=1)

        else:
            raise ValueError(f"Unknown or missing source type in config: {source_type}")

        return df.with_columns(
            pl.col("Pickup_DateTime").cast(pl.Datetime),
            pl.col("Delivery_Timestamp").cast(pl.Datetime)
        )

    def _generate_data(self, num_rows: int, progress=None, task_id=None) -> pl.DataFrame:
        """
        Generates synthetic delivery data in chunks to provide real-time progress.

        Args:
            num_rows: The total number of rows to generate.
            progress: A rich.progress object for updating the progress bar.
            task_id: The ID of the progress bar task to update.

        Returns:
            A Polars DataFrame with the generated synthetic data.
        """
        package_types = ["Small", "Medium", "Large", "Extra Large", "Special"]
        delivery_zones = ["Urban", "Suburban", "Rural", "Industrial", "Shopping Center"]

        all_data_chunks = []
        CHUNK_SIZE = 10000

        for i in range(0, num_rows, CHUNK_SIZE):
            current_chunk_size = min(CHUNK_SIZE, num_rows - i)

            pickup_datetimes = [self._fake.date_time_between(start_date="-30d", end_date="now") for _ in range(current_chunk_size)]
            delivery_timestamps = [p + timedelta(minutes=int(np.random.uniform(20, 360))) for p in pickup_datetimes]

            data_chunk = {
                "Delivery_ID": [f"SC{1000 + i + j}" for j in range(current_chunk_size)],
                "Pickup_DateTime": pickup_datetimes,
                "Delivery_Timestamp": delivery_timestamps,
                "Package_Type": np.random.choice(package_types, current_chunk_size, p=[0.4, 0.3, 0.15, 0.1, 0.05]),
                "Distance": np.random.uniform(1, 50, current_chunk_size).round(2),
                "Delivery_Zone": np.random.choice(delivery_zones, current_chunk_size, p=[0.35, 0.25, 0.2, 0.1, 0.1]),
            }
            all_data_chunks.append(pl.from_dict(data_chunk))

            if progress and task_id is not None:
                progress.update(task_id, advance=current_chunk_size)

        return pl.concat(all_data_chunks)
