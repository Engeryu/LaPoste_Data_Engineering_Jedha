# supercourier_etl/core/transform.py
import polars as pl

class Transformer:
    """Applies business logic to transform the data."""

    def __init__(self, config: dict):
        self.config = config

    def transform_data(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Applies a series of transformations to the input DataFrame.
        """
        print("Transforming data...")

        if df.is_empty():
            return df

        # Chain of transformations
        transformed_df = (
            df.pipe(self._calculate_delivery_duration)
              .pipe(self._add_temporal_features)
            # .pipe(self._enrich_with_weather_data)
            # .pipe(self._determine_delay_status)
        )

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
            
            (
                (duration_in_seconds // 60).cast(pl.Utf8) + "." + 
                (duration_in_seconds % 60).cast(pl.Utf8).str.pad_start(2, "0")
            ).alias("Actual_Delivery_Time_Display")
        )
        
        return df_with_formats
    
    def _add_temporal_features(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Adds time-based features like weekday and hour.
        """
        print("  -> Adding temporal features (weekday, hour)...")

        weekday_map = {
            1: "Monday", 2: "Tuesday", 3: "Wednesday",
            4: "Thursday", 5: "Friday", 6: "Saturday", 7: "Sunday"
        }

        df_with_features = df.with_columns(
            pl.col("Pickup_DateTime").dt.hour().alias("Hour"),
            
            # Utilisation de replace, maintenant que ton environnement est à jour.
            pl.col("Pickup_DateTime").dt.weekday().replace_strict(weekday_map, return_dtype=pl.Utf8).alias("Weekday")
        )
        return df_with_features

    def _determine_delay_status(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Applies the delay calculation formula to determine delivery status.
        A delivery is delayed if its actual time exceeds the theoretical time * 1.2.
        """
        print("  -> Determining delay status...")

        package_factor = (
            pl.when(pl.col("Package_Type") == "Small").then(1.0)
            .when(pl.col("Package_Type") == "Medium").then(1.2)
            .when(pl.col("Package_Type") == "Large").then(1.5)
            .when(pl.col("Package_Type") == "Extra Large").then(2.0)
            .when(pl.col("Package_Type") == "Special").then(2.5)
            .otherwise(1.0)
        )

        zone_factor = (
            pl.when(pl.col("Delivery_Zone") == "Urban").then(1.2)
            .when(pl.col("Delivery_Zone") == "Suburban").then(1.0)
            .when(pl.col("Delivery_Zone") == "Rural").then(1.3)
            .when(pl.col("Delivery_Zone") == "Industrial").then(0.9)
            .when(pl.col("Delivery_Zone") == "Shopping Center").then(1.4)
            .otherwise(1.0)
        )
        
        df_with_theoretical_time = df.with_columns(
            (
                (30 + pl.col("Distance") * 0.8) * package_factor * zone_factor
            ).alias("Theoretical_Time_Minutes")
        )
        
        # 3. Determine status (to be completed)
        
        return df_with_theoretical_time

    def _enrich_with_weather_data(self, df: pl.DataFrame) -> pl.DataFrame:
        """Fetches and merges weather data."""
        return df

    def _determine_delay_status(self, df: pl.DataFrame) -> pl.DataFrame:
        """Applies the delay calculation formula."""
        return df