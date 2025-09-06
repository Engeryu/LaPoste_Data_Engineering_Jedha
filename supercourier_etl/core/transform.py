# supercourier_etl/core/transform.py
"""
Logic Transformation of:
    - Delivery duration computation
    - Split DateTime feature for in-depths Temporal Features (Days & Hours)
    - Merging Weather with deliveries based on Day & Hour
    - Apply duration computation to categorize Delivery Status (Delayed or On-Time)
"""
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
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

    def transform_data(self, df: pl.DataFrame, progress=None, parent_task_id=None) -> pl.DataFrame:
        """
        Orchestrates the sequence of transformations on the input DataFrame.
        Manages sub-tasks for progress reporting.

        Args:
            df: The raw DataFrame from the Extractor.
            progress: A rich.progress object for updating the progress bar.
            parent_task_id: The ID of the parent progress bar task to update.

        Returns:
            The fully transformed and enriched DataFrame.
        """
        if df.is_empty():
            return df

        if progress:
            unique_dates_count = df.get_column("Pickup_DateTime").dt.date().n_unique()
            weather_task = progress.add_task("  -> [yellow]Fetching weather data...", total=unique_dates_count)
            logic_task = progress.add_task("  -> [magenta]Applying business logic...", total=3)
        else:
            weather_task, logic_task = None, None

        transformed_df = (
            df.pipe(self._add_temporal_features, progress=progress, task_id=logic_task)
              .pipe(self._enrich_with_weather_data, progress=progress, task_id=weather_task)
              .pipe(self._calculate_delivery_duration, progress=progress, task_id=logic_task)
              .pipe(self._determine_delay_status, progress=progress, task_id=logic_task)
        )

        if progress and parent_task_id is not None:
            progress.update(logic_task, description="  -> [magenta]Business logic applied")
            progress.update(parent_task_id, advance=1)

        return transformed_df

    def _fetch_weather_concurrently(self, unique_dates: list, progress=None, task_id=None) -> list:
        """Fetches weather data for a list of dates in parallel."""
        all_hourly_data = []
        location = "Paris"

        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_date = {executor.submit(self.weather_client.get_historical_weather, location, date.strftime("%Y-%m-%d")): date for date in unique_dates}

            for future in as_completed(future_to_date):
                date_obj = future_to_date[future]
                try:
                    weather_data = future.result()
                    if weather_data and "forecast" in weather_data:
                        hourly = weather_data["forecast"]["forecastday"][0]["hour"]
                        for hour_data in hourly:
                            all_hourly_data.append({
                                "date": date_obj, "Hour": int(hour_data["time"].split(" ")[1].split(":")[0]),
                                "Weather_Condition": hour_data["condition"]["text"]
                            })
                except requests.exceptions.RequestException as exc: 
                    print(f"\nA weather request for {date_obj} generated an exception: {exc}")

                if progress and task_id is not None:
                    progress.advance(task_id)

        return all_hourly_data

    def _enrich_with_weather_data(self, df: pl.DataFrame, progress=None, task_id=None) -> pl.DataFrame:
        """Enriches the DataFrame with historical weather data using concurrent calls."""
        unique_dates = df.get_column("Pickup_DateTime").dt.date().unique().to_list()

        all_hourly_data = self._fetch_weather_concurrently(unique_dates, progress, task_id)

        if not all_hourly_data:
            return df.with_columns(pl.lit(None, dtype=pl.Utf8).alias("Weather_Condition"))

        weather_df = pl.from_records(all_hourly_data)

        df_with_join_key = df.with_columns(pl.col("Pickup_DateTime").dt.date().alias("date"))

        df_enriched = df_with_join_key.join(
            weather_df, on=["date", "Hour"], how="left"
        ).drop("date")

        if progress and task_id is not None:
            progress.update(task_id, description="  -> [green]Weather data enriched")

        return df_enriched

    def _calculate_delivery_duration(self, df: pl.DataFrame, progress=None, task_id=None) -> pl.DataFrame:
        """
        Calculates delivery duration in both numeric and display formats.
        """
        duration_in_seconds = (pl.col("Delivery_Timestamp") - pl.col("Pickup_DateTime")).dt.total_seconds()
        df_with_formats = df.with_columns(
            (duration_in_seconds / 60).round(2).alias("Actual_Delivery_Time_Minutes"),
            ((duration_in_seconds // 60).cast(pl.Utf8) + "." + (duration_in_seconds % 60).cast(pl.Utf8).str.pad_start(2, "0")).alias("Actual_Delivery_Time_Display")
        )
        if progress and task_id is not None:
            progress.advance(task_id)

        return df_with_formats

    def _add_temporal_features(self, df: pl.DataFrame, progress=None, task_id=None) -> pl.DataFrame:
        """
        Adds time-based features ('Hour', 'Weekday') based on the pickup time.
        """
        weekday_map = {1: "Monday", 2: "Tuesday", 3: "Wednesday", 4: "Thursday", 5: "Friday", 6: "Saturday", 7: "Sunday"}
        df_with_features = df.with_columns(
            pl.col("Pickup_DateTime").dt.hour().alias("Hour"),
            pl.col("Pickup_DateTime").dt.weekday().replace_strict(weekday_map, return_dtype=pl.Utf8).alias("Weekday")
        )
        if progress and task_id is not None:
            progress.advance(task_id)

        return df_with_features

    def _determine_delay_status(self, df: pl.DataFrame, progress=None, task_id=None) -> pl.DataFrame:
        """
        Applies the delay calculation formula to determine delivery status.
        """
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
            pl.when(pl.col("Weather_Condition").is_null()).then(1.0)
            .when(pl.col("Weather_Condition").str.contains("(?i)rain|drizzle")).then(1.2)
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
        if progress and task_id is not None:
            progress.advance(task_id)

        return df_with_status
