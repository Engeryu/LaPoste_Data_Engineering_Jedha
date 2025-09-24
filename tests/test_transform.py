# tests/test_transform.py
import polars as pl
from polars.testing import assert_frame_equal
import os
from datetime import datetime
from supercourier_etl.core.transform import Transformer

def test_determine_delay_status():
    """
    Unit test for the _determine_delay_status method.
    
    It creates a small, controlled DataFrame and asserts that the calculated
    theoretical time and status are correct based on the business logic.
    """
    # 1. Setup
    # Mock config and instantiate the transformer
    # We need to set a dummy API key for the client to initialize
    os.environ["WEATHERAPI_KEY"] = "dummy_key"
    config = {}
    transformer = Transformer(config)

    # Create a sample DataFrame with known values for calculation
    test_data = pl.DataFrame({
        "Distance": [10.0],
        "Package_Type": ["Large"],
        "Delivery_Zone": ["Urban"],
        "Hour": [8], # Morning peak
        "Weekday": ["Monday"], # Weekday peak
        "Weather_Condition": ["Light rain"],
        "Actual_Delivery_Time_Minutes": [100.0]
    })
    
    # 2. Execution
    result_df = transformer._determine_delay_status(test_data)

    # 3. Assertion
    # Base time = 30 + 10.0 * 0.8 = 38
    # Factors: package=1.5, zone=1.2, hour=1.3, day=1.2, weather=1.2
    # Theoretical = 38 * 1.5 * 1.2 * 1.3 * 1.2 * 1.2 = 128.04
    # Threshold = 127.3 * 1.2 = 152.76
    # Since Actual (100.0) < Threshold (152.76), status should be "On-time"

    expected_df = test_data.with_columns(
        pl.lit(128.04, dtype=pl.Float64).round(2).alias("Theoretical_Time_Minutes"),
        pl.lit("On-time", dtype=pl.Utf8).alias("Status")
    )
    
    # Use Polars' testing utility to compare DataFrames
    assert_frame_equal(result_df, expected_df)