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
        """
        Calculates delivery duration in two formats:
        1.  Decimal minutes for analysis (e.g., 90.5).
        2.  A string "minutes.seconds" for display (e.g., "90.30").
        """
        print("  -> Calculating delivery duration (numeric and display formats)...")
        
        duration_in_seconds = (
            (pl.col("Delivery_Timestamp") - pl.col("Pickup_DateTime"))
            .dt.total_seconds()
        )

        df_with_formats = df.with_columns(
            (duration_in_seconds / 60)
            .round(2)
            .alias("Actual_Delivery_Time_Minutes"),
            
            # 2. The string column for pure display
            (
                (duration_in_seconds // 60).cast(pl.Utf8) + "." + 
                (duration_in_seconds % 60).cast(pl.Utf8).str.pad_start(2, "0")
            ).alias("Actual_Delivery_Time_Display")
        )
        
        return df_with_formats

    def _enrich_with_weather_data(self, df: pl.DataFrame) -> pl.DataFrame:
        """Fetches and merges weather data."""
        # Logic for API calls will be added here
        return df

    def _determine_delay_status(self, df: pl.DataFrame) -> pl.DataFrame:
        """Applies the delay calculation formula."""
        # Business logic for delay status will be added here
        return df