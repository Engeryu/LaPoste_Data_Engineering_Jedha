# tests/test_pipeline.py
import polars as pl
import os
import json
from supercourier_etl.pipeline import Pipeline

def test_full_pipeline_run(tmp_path):
    """
    Integration test for the entire ETL pipeline.

    It creates a temporary source file, runs the pipeline, and checks
    that the output files (data and manifest) are created correctly.

    Args:
        tmp_path: A pytest fixture that provides a temporary directory path.
    """
    # 1. Setup: Create a temporary source file and output directory
    source_dir = tmp_path / "source"
    output_dir = tmp_path / "output"
    source_dir.mkdir()
    output_dir.mkdir()
    
    source_csv_path = source_dir / "test_data.csv"
    output_base_path = str(output_dir / "result")

    # Create dummy data for the source file
    dummy_df = pl.DataFrame({
        "Delivery_ID": ["SC001"],
        "Pickup_DateTime": ["2025-09-05T10:00:00"],
        "Delivery_Timestamp": ["2025-09-05T10:45:00"],
        "Package_Type": ["Small"],
        "Distance": [5.0],
        "Delivery_Zone": ["Suburban"],
    })
    dummy_df.write_csv(source_csv_path)

    # 2. Execution: Run the pipeline
    config = {
        "source": {"type": "file", "path": str(source_csv_path)},
        "output": {"path": output_base_path, "format": "csv"}
    }
    
    # We need a dummy API key for the pipeline to initialize
    os.environ["WEATHERAPI_KEY"] = "dummy_key_for_test"

    pipeline = Pipeline(config)
    pipeline.run()

    # 3. Assertion: Check if output files were created
    output_csv_path = f"{output_base_path}.csv"
    manifest_path = f"{output_base_path}_manifest.json"

    assert os.path.exists(output_csv_path)
    assert os.path.exists(manifest_path)

    # Optional: Check the content of the outputs
    result_df = pl.read_csv(output_csv_path)
    assert result_df.shape == (1, 13) # 6 original + 7 new columns
    assert "Status" in result_df.columns

    with open(manifest_path, 'r') as f:
        manifest = json.load(f)
    assert manifest["polars_version"] is not None
    assert manifest["dataset_shape"]["rows"] == 1