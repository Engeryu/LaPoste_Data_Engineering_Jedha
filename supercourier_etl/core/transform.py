# supercourier_etl/core/transform.py
import polars as pl

class Transformer:
    """Applies business logic to transform the data."""

    def __init__(self, config: dict):
        self.config = config

    def transform_data(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Applies a series of transformations to the input DataFrame.

        Args:
            df (pl.DataFrame): The raw data DataFrame.

        Returns:
            pl.DataFrame: The transformed DataFrame.
        """
        print("Transforming data...")

        if df.is_empty():
            return df

        transformed_df = df.pipe(self._calculate_delivery_duration)
        # .pipe(self._enrich_with_weather_data)
        # .pipe(self._determine_delay_status)

        return transformed_df

    def _calculate_delivery_duration(self, df: pl.DataFrame) -> pl.DataFrame:
        """Calculates the actual delivery time."""
        print(" -> Calculating delivery durations...")

        df_with_duration = df.with_columns(
            (
                (pl.col("Delivery_Timestamp") - pl.col("Pickup_DateTime"))
                .dt.total_minutes()
                .cast(pl.Int64)
                .alias("Actual_Delivery_Time")
            )
        )
        return df_with_duration

    def _enrich_with_weather_data(self, df: pl.DataFrame) -> pl.DataFrame:
        """Fetches and merges weather data."""
        # Logic for API calls will be added here
        return df

    def _determine_delay_status(self, df: pl.DataFrame) -> pl.DataFrame:
        """Applies the delay calculation formula."""
        # Business logic for delay status will be added here
        return df