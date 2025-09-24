# supercourier_etl/core/load.py
"""
    
"""
import os
import json
from datetime import datetime, timezone
import polars as pl
from ..sources import writers

class Loader:
    """
    Handles the final step of the ETL: loading the data to its destination.
    This includes writing to various file formats and generating a metadata manifest.
    """
    WRITER_MAP = {
        "csv": writers.CsvWriter,
        "json": writers.JsonWriter,
        "parquet": writers.ParquetWriter,
        "db": writers.DatabaseWriter,
        "xlsx": writers.XlsxWriter
    }

    def __init__(self, config: dict):
        """
        Initializes the Loader with the pipeline configuration.

        Args:
            config: The pipeline configuration dictionary.
        """
        self.config = config

    def load_data(self, df: pl.DataFrame, progress=None, parent_task_id=None):
        """
        Saves the DataFrame to file(s) and generates a manifest.

        Based on the output configuration, this method either prints a preview
        to the console or writes the data to one or more specified file formats.
        A manifest file is always generated regardless of the output format.

        Args:
            df: The final, transformed DataFrame to be loaded.
        """
        output_conf = self.config.get("output", {})
        output_format = output_conf.get("format", "preview")
        base_path = output_conf.get("path", "output/default_name")

        os.makedirs(os.path.dirname(base_path), exist_ok=True)

        if output_format == "preview":
            print("\n--- Preview of Final DataFrame ---")
            print(df.head())
        else:
            formats_to_write = self._get_formats_to_write(output_format)

            if progress:
                load_task = progress.add_task("[cyan]Writing output files...", total=len(formats_to_write))

            for fmt in formats_to_write:
                if progress:
                    progress.update(load_task, description=f"[cyan]Writing {fmt.upper()} file...")

                writer_class = self.WRITER_MAP.get(fmt)
                if writer_class:
                    writer = writer_class(base_path)
                    writer.write(df)

                if progress:
                    progress.advance(load_task)

            if progress:
                progress.update(load_task, description="[cyan]All files written.")

        self._generate_manifest(df, output_conf)

        if progress and parent_task_id is not None:
            progress.advance(parent_task_id)

    def _get_formats_to_write(self, format_str: str) -> list[str]:
        """
        Determines which writers to use based on the format string.

        Args:
            format_str: The format string from the configuration (e.g., "csv", "all").

        Returns:
            A list of format keys to be written.
        """
        if format_str == "all":
            return list(self.WRITER_MAP)
        if format_str == "all_but_xlsx":
            return [f for f in self.WRITER_MAP if f != "xlsx"]

        return [format_str]

    def _generate_manifest(self, df: pl.DataFrame, output_conf: dict):
        """
        Generates a JSON manifest file with metadata about the run.

        Args:
            df: The DataFrame being loaded, used to extract shape and column info.
            output_conf: The output configuration dictionary.
        """
        base_path = output_conf.get("path", "output/default_name")
        manifest_path = f"{base_path}_manifest.json"

        print(f"  -> Generating metadata manifest at {manifest_path}...")

        manifest_data = {
            "polars_version": pl.__version__,
            "run_timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "source_config": self.config.get("source"),
            "output_config": output_conf,
            "dataset_shape": {"rows": df.height, "columns": df.width},
            "columns": df.columns,
        }

        with open(manifest_path, 'w', encoding="utf-8") as f:
            json.dump(manifest_data, f, indent=4)
