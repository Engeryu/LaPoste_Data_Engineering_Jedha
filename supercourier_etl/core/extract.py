# supercourier_etl/core/extract.py
import polars as pl
import numpy as np
from faker import Faker
from datetime import timedelta

class Extractor:
    """Handles the data extraction from various sources."""

    def __init__(self, config: dict):
        self.config = config
        self._fake = Faker()

    def extract_data(self) -> pl.DataFrame:
        """
        Extracts data based on the configuration.
        It can either generate data or read from a specified source file.

        Returns:
            pl.DataFrame: The extracted data as a Polars DataFrame.
        """
        if "generate" in self.config:
            num_rows = self.config["generate"]["rows"]
            print(f"Generating {num_rows} synthetic records...")
            return self._generate_data(num_rows)
        
        # The logic for reading files will be added here later
        elif "source" in self.config:
            raise NotImplementedError("File reading is not yet implemented.")
        
        else:
            raise ValueError("Configuration must specify either 'generate' or 'source'.")

    def _generate_data(self, num_rows: int) -> pl.DataFrame:
        """
        Generates a synthetic dataset of delivery records.

        This simulates data from two sources: a logistics DB (ID, package info)
        and tracking logs (timestamps).

        Args:
            num_rows (int): The number of records to generate.

        Returns:
            pl.DataFrame: A Polars DataFrame with synthetic data.
        """
        package_types = ["Small", "Medium", "Large", "Extra Large", "Special"]
        delivery_zones = ["Urban", "Suburban", "Rural", "Industrial", "Shopping Center"]

        # Generate pickup times first to calculate delivery times based on them
        pickup_datetimes = [
            self._fake.date_time_between(start_date="-30d", end_date="now")
            for _ in range(num_rows)
        ]
        
        # Simulate delivery times: random duration between 20 mins and 6 hours
        delivery_timestamps = [
            pickup + timedelta(minutes=int(np.random.uniform(20, 360)))
            for pickup in pickup_datetimes
        ]

        data = {
            "Delivery_ID": [f"SC{1000 + i}" for i in range(num_rows)],
            "Pickup_DateTime": pickup_datetimes,
            "Delivery_Timestamp": delivery_timestamps,
            "Package_Type": np.random.choice(
                package_types, num_rows, p=[0.4, 0.3, 0.15, 0.1, 0.05]
            ),
            "Distance": np.random.uniform(1, 50, num_rows).round(2),
            "Delivery_Zone": np.random.choice(
                delivery_zones, num_rows, p=[0.35, 0.25, 0.2, 0.1, 0.1]
            ),
        }

        return pl.from_dict(data)