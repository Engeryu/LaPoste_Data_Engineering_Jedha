# supercourier_etl/sources/writers.py
import polars as pl
from abc import ABC, abstractmethod

class BaseWriter(ABC):
    """Abstract base class for all file writers."""
    
    def __init__(self, base_path: str):
        self.base_path = base_path

    @abstractmethod
    def write(self, df: pl.DataFrame):
        """Writes the DataFrame to a specific file format."""
        pass

class CsvWriter(BaseWriter):
    def write(self, df: pl.DataFrame):
        path = f"{self.base_path}.csv"
        print(f"    -> Writing to {path}")
        df.write_csv(path)

class JsonWriter(BaseWriter):
    def write(self, df: pl.DataFrame):
        path = f"{self.base_path}.json"
        print(f"    -> Writing to {path}")
        df.write_json(path, row_oriented=True)

class ParquetWriter(BaseWriter):
    def write(self, df: pl.DataFrame):
        path = f"{self.base_path}.parquet"
        print(f"    -> Writing to {path}")
        df.write_parquet(path)

class XlsxWriter(BaseWriter):
    def write(self, df: pl.DataFrame):
        path = f"{self.base_path}.xlsx"
        print(f"    -> Writing to {path}")
        df.write_excel(path)