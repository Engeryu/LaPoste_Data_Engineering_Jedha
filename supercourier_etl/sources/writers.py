# supercourier_etl/sources/writers.py
"""
    All File Format Writting Functions:
        - Comma-Separate Virgule
        - JSON
        - Parquet
        - Database (SQLite3)
        - XLSX
"""
from abc import ABC, abstractmethod
import polars as pl
import xlsxwriter

class BaseWriter(ABC):
    """Abstract base class for all file writers."""
    def __init__(self, base_path: str):
        self.base_path = base_path

    @abstractmethod
    def write(self, df: pl.DataFrame):
        """Writes the DataFrame to a specific file format."""
        return df

class CsvWriter(BaseWriter):
    """Writes data to a CSV file for Data Scientists"""
    def write(self, df: pl.DataFrame):
        path = f"{self.base_path}.csv"
        print(f"    -> Writing to {path}")
        df.write_csv(path)

class JsonWriter(BaseWriter):
    """Writes data to a newline-delimited JSON file for memory efficiency."""
    def write(self, df: pl.DataFrame):
        path = f"{self.base_path}.json"
        print(f"    -> Writing to {path} (ndjson format)")
        df.write_ndjson(path)

class ParquetWriter(BaseWriter):
    """Writes heavier data to a table in parquet for Data Scientists"""
    def write(self, df: pl.DataFrame):
        path = f"{self.base_path}.parquet"
        print(f"    -> Writing to {path}")
        df.write_parquet(path)

class DatabaseWriter(BaseWriter):
    """Writes data to a table in a SQLite database in chunks."""
    def write(self, df: pl.DataFrame):
        table_name = "deliveries"
        path = f"{self.base_path}.db"
        connection_string = f"sqlite:///{path}"
        print(f"    -> Writing to table '{table_name}' in {path}")

        # Write in chunks to manage memory
        for chunk in df.iter_slices():
            chunk.write_database(
            table_name=table_name,
            connection=connection_string,
            if_table_exists="append"
            )

class XlsxWriter(BaseWriter):
    """Writes data to an Excel file using a streaming approach for performance."""
    def write(self, df: pl.DataFrame):
        path = f"{self.base_path}.xlsx"
        print(f"    -> Writing to {path} (streaming)")
        with xlsxwriter.Workbook(path, {'constant_memory': True}) as workbook:
            worksheet = workbook.add_worksheet()
            worksheet.write_row(0, 0, df.columns)
            for i, row in enumerate(df.iter_rows()):
                worksheet.write_row(i + 1, 0, row)
