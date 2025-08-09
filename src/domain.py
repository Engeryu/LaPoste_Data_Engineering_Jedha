# supercourier_etl/domain.py
"""
Domain Logic Module for the SuperCourier ETL pipeline.\n
This module centralizes the core business logic, coefficients, and calculations.\n
By isolating these rules, the system becomes easier to maintain and update as business requirements evolve.\n
"""

# Imports of the necessary libraries
import pandas as pd
import numpy as np

# --- Business Coefficients ---
"""
These dictionaries define the core coefficients for the business logic.\n
They are used in vectorized calculations to adjust theoretical delivery times.\n
- PACKAGE_TYPE_COEFFS: Multipliers based on the size and type of the package.\n
- DELIVERY_ZONE_COEFFS: Multipliers for different delivery destinations.\n
- WEATHER_CONDITION_COEFFS: Multipliers that account for weather-related delays.\n
- DAY_TYPE_COEFFS: Multipliers for weekday vs. weekend deliveries.\n
- PEAK_HOUR_COEFFS: Multipliers for deliveries during different time periods.\n
"""
PACKAGE_TYPE_COEFFS = {'Small': 1.0, 'Medium': 1.2, 'Large': 1.5, 'Extra Large': 2.0, 'Special': 2.5}
DELIVERY_ZONE_COEFFS = {'Urban': 1.2, 'Suburban': 1.0, 'Rural': 1.3, 'Industrial': 0.9, 'Shopping Center': 1.4}
WEATHER_CONDITION_COEFFS = {'Sunny': 1.0, 'Cloudy': 1.05, 'Rainy': 1.2, 'Stormy': 1.5, 'Snowy': 1.8}
DAY_TYPE_COEFFS = {'Weekday': 1.1, 'Weekend': 0.9}
PEAK_HOUR_COEFFS = {'Morning Peak': 1.3, 'Evening Peak': 1.4, 'Off-Peak': 1.0, 'Night': 1.2}

#==================================================================================
# --- Business Time-Sensitive Functions ---
#==================================================================================

def get_peak_hour_type(hour: int) -> str:
    """
    Categorizes a single hour into a specific delivery period.\n
    This function serves as a reference and is not used in the main vectorized calculation for performance reasons.\n
    The logic is implemented directly within `calculate_delivery_status` using `numpy.select`.\n
    """
    if 7 <= hour <= 9: return 'Morning Peak'
    if 17 <= hour <= 19: return 'Evening Peak'
    if 20 <= hour or hour < 7: return 'Night'
    return 'Off-Peak'

def calculate_delivery_status(df: pd.DataFrame) -> pd.DataFrame:
    """Calculates the final delivery status for each record in the DataFrame.\n
    This is the core function where all business rules are applied using efficient, vectorized operations.\n
    It performs the following steps:\n
    - Determines the peak hour type for each delivery using `numpy.select` for performance.\n
    - Calculates an adjusted theoretical delivery time by applying all relevant coefficients.\n
    - Compares the actual delivery time against a calculated delay threshold.\n
    - Assigns a final status of 'On-time' or 'Delayed' to each delivery.\n
    Returns the DataFrame with the new 'Status' and 'Peak_Hour_Type' columns.\n
    """
    # Vectorized way to determine peak hour type
    conditions = [
        (df['Hour'] >= 7) & (df['Hour'] <= 9),
        (df['Hour'] >= 17) & (df['Hour'] <= 19),
        (df['Hour'] >= 20) | (df['Hour'] < 7)
    ]
    choices = ['Morning Peak', 'Evening Peak', 'Night']
    df['Peak_Hour_Type'] = np.select(conditions, choices, default='Off-Peak')

    # Map coefficients to the DataFrame to prepare for vectorized calculation, performed on all rows simultaneously for maximum efficiency.
    base_theoretical_time = 30 + df['Distance'] * 0.8
    adjusted_theoretical_time = (
        base_theoretical_time *
        df['Package_Type'].map(PACKAGE_TYPE_COEFFS) *
        df['Delivery_Zone'].map(DELIVERY_ZONE_COEFFS) *
        df['Weather_Condition'].map(WEATHER_CONDITION_COEFFS).fillna(1.0) *
        df['Day_Type'].map(DAY_TYPE_COEFFS) *
        df['Peak_Hour_Type'].map(PEAK_HOUR_COEFFS)
    )
    delay_threshold = adjusted_theoretical_time * 1.2

    df['Status'] = np.where(df['Actual_Delivery_Time_Minutes'] > delay_threshold, 'Delayed', 'On-time')
    return df