# supercourier_etl/core/transform.py
"""Logic Transformation of:
    - Delivery duration computation
    - Split DateTime feature for in-depths Temporal Features (Days & Hours)
    - Merging Weather with deliveries based on Day & Hour
    - Apply duration computation to categorize Delivery Status (Delayed or On-Time) """

import os
import polars as pl
from ..utils.api_client import WeatherAPIClient

class Transformer:
    """
    Applies all business logic transformations to the delivery data.
    """

    def __init__(self, config: dict):
        """
        Initializes the Transformer, setting up the API client.

        Args:
            config: The pipeline configuration dictionary.
        """
        self.config = config
        api_key = os.getenv("WEATHERAPI_KEY")
        self.weather_client = WeatherAPIClient(api_key)

    def transform_data(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Orchestrates the sequence of transformations on the input DataFrame.

        Args:
            df: The raw DataFrame from the Extractor.

        Returns:
            The fully transformed and enriched DataFrame.
        """
        print("Transforming data...")
        if df.is_empty():
            return df

        transformed_df = (
            df.pipe(self._calculate_delivery_duration)
              .pipe(self._add_temporal_features)
              .pipe(self._enrich_with_weather_data)
              .pipe(self._determine_delay_status)
        )
        return transformed_df

    def _calculate_delivery_duration(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Calculates delivery duration in both numeric and display formats.

        Args:
            df: The input DataFrame with 'Pickup_DateTime' and 'Delivery_Timestamp'.

        Returns:
            The DataFrame with added 'Actual_Delivery_Time_Minutes' (float)
            and 'Actual_Delivery_Time_Display' (string) columns.
        """
        print("  -> Calculating delivery duration (numeric and display formats)...")

        duration_in_seconds = (
            (pl.col("Delivery_Timestamp") - pl.col("Pickup_DateTime"))
            .dt.total_seconds()
        )

        df_with_formats = df.with_columns(
            (duration_in_seconds / 60).round(2).alias("Actual_Delivery_Time_Minutes"),
            ((duration_in_seconds // 60).cast(pl.Utf8) + "." + (duration_in_seconds % 60).cast(pl.Utf8).str.pad_start(2, "0")).alias("Actual_Delivery_Time_Display")
        )
        return df_with_formats

    def _add_temporal_features(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Adds time-based features ('Hour', 'Weekday') based on the pickup time.

        Args:
            df: The input DataFrame with a 'Pickup_DateTime' column.

        Returns:
            The DataFrame with added 'Hour' and 'Weekday' columns.
        """
        print("  -> Adding temporal features (weekday, hour)...")
        weekday_map = {1: "Monday", 2: "Tuesday", 3: "Wednesday", 4: "Thursday", 5: "Friday", 6: "Saturday", 7: "Sunday"}
        df_with_features = df.with_columns(
            pl.col("Pickup_DateTime").dt.hour().alias("Hour"),
            pl.col("Pickup_DateTime").dt.weekday().replace_strict(weekday_map, return_dtype=pl.Utf8).alias("Weekday")
        )
        return df_with_features

    def _enrich_with_weather_data(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Enriches the DataFrame with historical weather data from WeatherAPI.

        Optimizes API calls by fetching data once per unique date. Assumes a single
        pickup location for all deliveries.

        Args:
            df: The DataFrame with 'Pickup_DateTime' and 'Hour' columns.

        Returns:
            The DataFrame enriched with a 'Weather_Condition' column.
        """
        print("  -> Enriching with weather data (this may take a moment)...")
        location = "Paris"
        unique_dates = df.select(pl.col("Pickup_DateTime").dt.date().unique()).to_series()

        all_hourly_data = []
        for date_obj in unique_dates:
            date_str = date_obj.strftime("%Y-%m-%d")
            weather_data = self.weather_client.get_historical_weather(location, date_str)
            if weather_data and "forecast" in weather_data:
                hourly = weather_data["forecast"]["forecastday"][0]["hour"]
                for hour_data in hourly:
                    all_hourly_data.append({
                        "date": date_obj, "Hour": int(hour_data["time"].split(" ")[1].split(":")[0]),
                        "Weather_Condition": hour_data["condition"]["text"]
                    })

        if not all_hourly_data:
            print("  -> No weather data fetched. Skipping enrichment.")
            return df.with_columns(pl.lit(None, dtype=pl.Utf8).alias("Weather_Condition"))

        weather_df = pl.DataFrame(all_hourly_data)
        df_with_join_key = df.with_columns(pl.col("Pickup_DateTime").dt.date().alias("date"))

        df_enriched = df_with_join_key.join(
            weather_df, on=["date", "Hour"], how="left"
        ).drop("date")

        return df_enriched

    def _determine_delay_status(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Applies the delay calculation formula to determine delivery status.

        A delivery is delayed if its actual time exceeds the theoretical time multiplied by 1.2.
        This method calculates the theoretical time based on various factors (package, zone,
        time of day, etc.) and compares it to the actual delivery time.

        Args:
            df: The input DataFrame enriched with temporal and weather data.

        Returns:
            The DataFrame with two new columns: 'Theoretical_Time_Minutes' and 'Status'.
        """
        print("  -> Determining delay status...")

        PACKAGE_FACTORS = {"Small": 1.0, "Medium": 1.2, "Large": 1.5, "Extra Large": 2.0, "Special": 2.5}
        ZONE_FACTORS = {"Urban": 1.2, "Suburban": 1.0, "Rural": 1.3, "Industrial": 0.9, "Shopping Center": 1.4}

        package_factor = pl.col("Package_Type").replace_strict(PACKAGE_FACTORS, default=1.0)
        zone_factor = pl.col("Delivery_Zone").replace_strict(ZONE_FACTORS, default=1.0)

        peak_hour_factor = (
            pl.when(pl.col("Hour").is_between(7, 9, closed="both")).then(1.3)
            .when(pl.col("Hour").is_between(17, 19, closed="both")).then(1.4)
            .otherwise(1.0)
        )
        day_factor = (
            pl.when(pl.col("Weekday").is_in(["Monday", "Friday"])).then(1.2)
            .when(pl.col("Weekday").is_in(["Saturday", "Sunday"])).then(0.9)
            .otherwise(1.0)
        )
        weather_factor = (
            pl.when(pl.col("Weather_Condition").str.contains("(?i)rain|drizzle")).then(1.2)
            .when(pl.col("Weather_Condition").str.contains("(?i)snow|blizzard|sleet")).then(1.8)
            .when(pl.col("Weather_Condition").str.contains("(?i)fog|mist")).then(1.1)
            .otherwise(1.0)
        )

        theoretical_time_expr = (
            (30 + pl.col("Distance") * 0.8)
            * package_factor * zone_factor
            * peak_hour_factor * day_factor * weather_factor
        )

        delay_threshold_expr = theoretical_time_expr * 1.2

        status_expr = (
            pl.when(pl.col("Actual_Delivery_Time_Minutes") > delay_threshold_expr)
            .then(pl.lit("Delayed"))
            .otherwise(pl.lit("On-time"))
            .alias("Status")
        )

        df_with_status = df.with_columns(
            theoretical_time_expr.round(2).alias("Theoretical_Time_Minutes"),
            status_expr
        )

        return df_with_status
