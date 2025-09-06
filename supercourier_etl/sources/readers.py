# supercourier_etl/sources/readers.py
"""
    All File Format Reading Functions:
        - Comma-Separate Virgule
        - JSON
        - Parquet
        - Database (SQLite3)
        - XLSX
"""
from abc import ABC, abstractmethod
import polars as pl

class BaseReader(ABC):
    """Abstract base class for all file readers."""

    def __init__(self, path: str):
        """
        Initializes the reader with the path to the source file.

        Args:
            path: The full path to the file to be read.
        """
        self.path = path

    @abstractmethod
    def read(self) -> pl.DataFrame:
        """Reads a file and returns its content as a Polars DataFrame."""
        return pl

class CsvReader(BaseReader):
    """Reads data from a CSV file."""
    def read(self) -> pl.DataFrame:
        return pl.read_csv(self.path)

class JsonReader(BaseReader):
    """Reads data from a JSON file."""
    def read(self) -> pl.DataFrame:
        return pl.read_json(self.path)

class ParquetReader(BaseReader):
    """Reads data from a Parquet file."""
    def read(self) -> pl.DataFrame:
        return pl.read_parquet(self.path)

class DatabaseReader(BaseReader):
    """Reads data from a database using a SQL query."""
    def read(self) -> pl.DataFrame:
        query = "SELECT * FROM deliveries"
        connection_string = f"sqlite:///{self.path}"
        return pl.read_database(query=query, connection=connection_string)

class XlsxReader(BaseReader):
    """Reads data from an Excel (.xlsx) file."""
    def read(self) -> pl.DataFrame:
        return pl.read_excel(self.path)
