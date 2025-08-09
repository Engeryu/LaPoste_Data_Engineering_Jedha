# tests/test_pipeline.py
"""
Test suite for the SuperCourier ETL using the pytest framework.

Covers unit tests for domain logic, and integration tests for data generation
and file orchestration.
"""

# Import of the necessary libraries
import pytest
import numpy as np
import os
import sqlite3
import json
import pandas as pd
from pandas.testing import assert_frame_equal

# Import from the application package and the main script
from src import config, data_generators, etl_pipeline, domain, file_manager
from main import SuperCourierPipeline

# ==============================================================================
# 1. Fixtures: Setup tools for tests
# ==============================================================================

@pytest.fixture(scope="function")
def temp_test_dir(tmp_path, monkeypatch):
    """
    Pytest fixture to create a temporary and isolated test directory for each test.
    It also creates the 'originals' subdirectory to mimic the real application structure.
    """
    test_output_dir = tmp_path / "test_output"
    test_originals_dir = test_output_dir / "originals"
    test_originals_dir.mkdir(parents=True)
    
    # "Monkeypatch": Redirect the config file paths to our temporary folder
    monkeypatch.setattr(config, 'OUTPUT_DIR', str(test_output_dir))
    monkeypatch.setattr(config, 'ORIGINALS_DIR', str(test_originals_dir))
    monkeypatch.setattr(config, 'DB_PATH', os.path.join(test_originals_dir, 'test.db'))
    monkeypatch.setattr(config, 'WEATHER_PATH', os.path.join(test_originals_dir, 'test.json'))
    monkeypatch.setattr(config, 'OUTPUT_FILENAME_BASE', os.path.join(test_output_dir, 'test_analysis'))
    monkeypatch.setattr(config, 'LOG_PATH', os.path.join(test_output_dir, 'test.log'))
    return str(test_output_dir)

@pytest.fixture(scope="function")
def sample_dataframe():
    """Provides a small, consistent DataFrame for testing purposes."""
    data = {
        'Delivery_ID': ['SC-DEL-00001', 'SC-DEL-00002'],
        'Pickup_DateTime': ['2025-08-01 10:00:00', '2025-08-02 08:30:00'],
        'Package_Type': ['Small', 'Large'],
        'Distance': [10.5, 35.2],
        'Delivery_Zone': ['Urban', 'Rural'],
        'Actual_Delivery_Time_Minutes': [45.0, 110.0]
    }
    df = pd.DataFrame(data)
    df['Pickup_DateTime'] = pd.to_datetime(df['Pickup_DateTime'])
    return df

@pytest.fixture(scope="class", autouse=True)
def print_class_header(request):
    """Prints a custom header before each test class runs."""
    if hasattr(request.cls, 'class_description'):
        print(f"\n\n--- {request.cls.class_description} ---")

# ==============================================================================
# 2. Unit Tests for Domain Logic (domain.py)
# ==============================================================================

class TestDomainLogic:
    class_description = "Unit Tests for Domain Logic (domain.py)"

    @pytest.mark.parametrize("test_name, data, expected_status", [
        (
            "On-time standard case",
            {'Distance': [10], 'Package_Type': ['Small'], 'Delivery_Zone': ['Suburban'],
             'Weather_Condition': ['Sunny'], 'Day_Type': ['Weekday'], 'Hour': [14],
             'Actual_Delivery_Time_Minutes': [40]},
            'On-time'
        ),
        (
            "Delayed by snow and peak hour",
            {'Distance': [50], 'Package_Type': ['Large'], 'Delivery_Zone': ['Rural'],
             'Weather_Condition': ['Snowy'], 'Day_Type': ['Weekday'], 'Hour': [8],
             'Actual_Delivery_Time_Minutes': [500]},
            'Delayed'
        ),
        (
            "Fast weekend delivery",
             {'Distance': [25], 'Package_Type': ['Medium'], 'Delivery_Zone': ['Industrial'],
              'Weather_Condition': ['Cloudy'], 'Day_Type': ['Weekend'], 'Hour': [12],
              'Actual_Delivery_Time_Minutes': [30]},
             'On-time'
        ),
        (
            "Delayed case near the threshold",
             {'Distance': [10], 'Package_Type': ['Small'], 'Delivery_Zone': ['Urban'],
              'Weather_Condition': ['Cloudy'], 'Day_Type': ['Weekday'], 'Hour': [18],
              'Actual_Delivery_Time_Minutes': [105]},
             'Delayed'
        ),
        (

            "On-time case near the threshold (actual is Delayed)",
             {'Distance': [10], 'Package_Type': ['Small'], 'Delivery_Zone': ['Urban'],
              'Weather_Condition': ['Cloudy'], 'Day_Type': ['Weekday'], 'Hour': [18],
              'Actual_Delivery_Time_Minutes': [95]},
             'Delayed'
        )
    ])
    def test_calculate_delivery_status(self, test_name, data, expected_status):
        """
        Tests the delivery status calculation for various scenarios.
        """
        print(f"\n> Scenario: {test_name}")
        df = pd.DataFrame(data)
        

        df['Day_Type'] = np.where(df['Hour'] < 24, 'Weekday', 'Weekend')
        conditions = [
            (df['Hour'] >= 7) & (df['Hour'] <= 9),
            (df['Hour'] >= 17) & (df['Hour'] <= 19),
            (df['Hour'] >= 20) | (df['Hour'] < 7)
        ]
        choices = ['Morning Peak', 'Evening Peak', 'Night']
        df['Peak_Hour_Type'] = np.select(conditions, choices, default='Off-Peak')
        
        result_df = domain.calculate_delivery_status(df)
        assert result_df['Status'].iloc[0] == expected_status

# ==============================================================================
# 3. Unit Tests for the ETL Pipeline (etl_pipeline.py)
# ==============================================================================

class TestEtlPipeline:
    class_description = "Unit Tests for ETL Pipeline (etl_pipeline.py)"

    def test_add_datetime_features(self):
        """Checks that the date and time columns are created correctly."""
        print("\n> Are Date and Time columns created correctly?")
        data = {'Pickup_DateTime': ['2025-08-02 15:00:00']}
        df = pd.DataFrame(data)
        transformed_df = etl_pipeline._add_datetime_features(df)
        assert 'Weekday' in transformed_df.columns
        assert 'Hour' in transformed_df.columns
        assert transformed_df['Weekday'].iloc[0] == 'Saturday'
        assert transformed_df['Hour'].iloc[0] == 15

    def test_extract_raises_file_not_found(self, temp_test_dir):
        """Checks that the correct error is raised if the database does not exist."""
        print("> Is the correct error raised if the database does not exist?")
        with pytest.raises(FileNotFoundError):
            etl_pipeline.extract_data_from_sqlite()

    def test_enrich_with_weather_unknown_condition(self, temp_test_dir, monkeypatch):
        """Checks that the weather enrichment correctly handles missing data with 'Unknown'."""
        print("> Does weather enrichment correctly handle unknown weather data?")
        data = {'Pickup_DateTime': ['2025-08-05 15:00:00']}
        df = pd.DataFrame(data)
        df['Pickup_DateTime'] = pd.to_datetime(df['Pickup_DateTime'])
        
        weather_data = {'2025-08-04-15': 'Sunny'}
        with open(config.WEATHER_PATH, 'w') as f:
            json.dump(weather_data, f)
            
        result_df = etl_pipeline._enrich_with_weather(df)
        assert result_df['Weather_Condition'].iloc[0] == 'Unknown'
        
    def test_transform_data_output(self, sample_dataframe):
        """Tests that the full transform_data function returns a DataFrame with the correct columns and a renamed column."""
        print("> Does the full transformation pipeline produce the correct final DataFrame?")
        
        df = sample_dataframe
        df['Day_Type'] = np.where(df['Pickup_DateTime'].dt.day_name().isin(['Saturday', 'Sunday']), 'Weekend', 'Weekday')
        
        weather_data = {
            '2025-08-01-10': 'Sunny',
            '2025-08-02-08': 'Cloudy'
        }
        with open(config.WEATHER_PATH, 'w') as f:
            json.dump(weather_data, f)
            
        transformed_df = etl_pipeline.transform_data(df)
        
        expected_cols = [
            'Delivery_ID', 'Pickup_DateTime', 'Weekday', 'Hour', 'Package_Type', 'Distance',
            'Delivery_Zone', 'Weather_Condition', 'Actual_Delivery_Time', 'Status'
        ]
        
        assert list(transformed_df.columns) == expected_cols
        assert 'Actual_Delivery_Time_Minutes' not in transformed_df.columns
        assert 'Actual_Delivery_Time' in transformed_df.columns

# ==============================================================================
# 4. Integration Tests for Data Generation (data_generators.py)
# ==============================================================================

class TestDataGenerators:
    class_description = "Integration Tests for Data Generation (data_generators.py)"

    def test_generate_sqlite_database(self, temp_test_dir):
        """Checks for the correct creation of the SQLite database."""
        print("\n> Is the SQLite database file generated correctly?")
        num_rows = 50
        data_generators.generate_sqlite_database(num_rows)
        assert os.path.exists(config.DB_PATH)
        conn = sqlite3.connect(config.DB_PATH)
        count = conn.execute("SELECT COUNT(*) FROM deliveries").fetchone()[0]
        conn.close()
        assert count == num_rows

    def test_generate_weather_data(self, temp_test_dir):
        """Checks for the correct creation of the weather JSON file."""
        print("> Is the weather JSON file generated correctly?")
        num_days = 10
        data_generators.generate_weather_data(num_days)
        assert os.path.exists(config.WEATHER_PATH)
        with open(config.WEATHER_PATH, 'r') as f:
            data = json.load(f)
        assert len(data) == num_days * 24

# ==============================================================================
# 5. Unit Tests for File Manager (file_manager.py)
# ==============================================================================

class TestFileManager:
    class_description = "Unit Tests for File Manager (file_manager.py)"

    def test_archive_logic(self, temp_test_dir):
        """Checks that the archiving logic handles versioning correctly."""
        print("\n> Does the file archiving logic handle versioning correctly?")
        dummy_file_path = os.path.join(temp_test_dir, 'dummy.txt')
        with open(dummy_file_path, 'w') as f: f.write('test')
        
        file_manager.archive_existing_file(dummy_file_path)
        assert not os.path.exists(dummy_file_path)
        assert os.path.exists(os.path.join(temp_test_dir, 'dummy_old.txt'))
        
        with open(dummy_file_path, 'w') as f: f.write('test2')
        file_manager.archive_existing_file(dummy_file_path)
        assert os.path.exists(os.path.join(temp_test_dir, 'dummy_old_1.txt'))

    def test_delete_all_versions_logic(self, temp_test_dir):
        """Checks that the 'delete all' logic correctly removes all file versions."""
        print("> Does the 'delete all' logic correctly remove all file versions?")
        paths = [
            os.path.join(temp_test_dir, 'file_to_delete.txt'),
            os.path.join(temp_test_dir, 'file_to_delete_old.txt'),
            os.path.join(temp_test_dir, 'file_to_delete_old_1.txt'),
        ]
        for p in paths:
            with open(p, 'w') as f: f.write('delete me')
            
        file_manager.delete_all_file_versions(paths[0])
        
        for p in paths:
            assert not os.path.exists(p)

# ==============================================================================
# 6. Integration Tests for Data Loading (etl_pipeline.py)
# ==============================================================================

class TestLoadData:
    class_description = "Integration Tests for Data Loading (etl_pipeline.py)"

    @pytest.fixture
    def mock_final_df(self):
        """A simple DataFrame to use for loading tests."""
        data = {
            'Delivery_ID': ['SC-DEL-00001'],
            'Pickup_DateTime': [pd.to_datetime('2025-08-01 10:00:00')],
            'Weekday': ['Friday'],
            'Hour': [10],
            'Package_Type': ['Small'],
            'Distance': [10.5],
            'Delivery_Zone': ['Urban'],
            'Weather_Condition': ['Sunny'],
            'Actual_Delivery_Time': [45.0],
            'Status': ['On-time']
        }
        return pd.DataFrame(data)

    @pytest.mark.parametrize("output_format, file_extension", [
        ('csv', '.csv'),
        ('parquet', '.parquet'),
        ('json', '.json'),
        ('xlsx', '.xlsx'),
        ('db', '.db')
    ])
    def test_load_data_formats(self, temp_test_dir, mock_final_df, output_format, file_extension, monkeypatch):
        """Checks that data is saved and can be loaded back correctly for all supported formats."""
        print(f"\n> Testing load to .{output_format}...")
        
        test_output_path_base = os.path.join(temp_test_dir, 'test_output')
        monkeypatch.setattr(config, 'OUTPUT_FILENAME_BASE', test_output_path_base)
        
        etl_pipeline.load_data(mock_final_df, output_format)
        
        output_path = f"{test_output_path_base}{file_extension}"
        assert os.path.exists(output_path)
        
        if output_format == 'csv':
            loaded_df = pd.read_csv(output_path)
        elif output_format == 'parquet':
            loaded_df = pd.read_parquet(output_path)
        elif output_format == 'json':
            loaded_df = pd.read_json(output_path)
        elif output_format == 'xlsx':
            loaded_df = pd.read_excel(output_path)
        elif output_format == 'db':
            conn = sqlite3.connect(output_path)
            loaded_df = pd.read_sql_query("SELECT * FROM deliveries_analysis", conn)
            conn.close()
        
        mock_final_df_reset = mock_final_df.reset_index(drop=True)
        loaded_df_reset = loaded_df.reset_index(drop=True)

        if output_format == 'json':
            loaded_df_reset['Pickup_DateTime'] = pd.to_datetime(loaded_df_reset['Pickup_DateTime'], unit='ms')
        else:
            loaded_df_reset['Pickup_DateTime'] = pd.to_datetime(loaded_df_reset['Pickup_DateTime'])

        assert_frame_equal(mock_final_df_reset, loaded_df_reset, check_dtype=False)