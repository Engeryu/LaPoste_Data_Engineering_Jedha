# supercourier_etl/sources/writers.py
import polars as pl
import xlsxwriter
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
        df.write_json(path)

class ParquetWriter(BaseWriter):
    def write(self, df: pl.DataFrame):
        path = f"{self.base_path}.parquet"
        print(f"    -> Writing to {path}")
        df.write_parquet(path)

class DatabaseWriter(BaseWriter):
    """Writes data to a table in a SQLite database."""
    def write(self, df: pl.DataFrame):
        table_name = "deliveries"
        path = f"{self.base_path}.db"
        connection_string = f"sqlite:///{path}"
        
        print(f"    -> Writing to table '{table_name}' in {path}")
        
        df.write_database(
            table_name=table_name,
            connection=connection_string,
            if_table_exists="replace"
        )

class XlsxWriter(BaseWriter):
    """Writes data to an Excel file using a streaming approach for performance."""
    def write(self, df: pl.DataFrame):
        """
        Writes a DataFrame to an .xlsx file row by row to keep memory usage constant.

        Args:
            df: The DataFrame to be written.
        """
        path = f"{self.base_path}.xlsx"
        print(f"    -> Writing to {path} (streaming)")

        # 'constant_memory=True' is crucial for xlsxwriter's performance mode
        with xlsxwriter.Workbook(path, {'constant_memory': True}) as workbook:
            worksheet = workbook.add_worksheet()

            # Write header
            worksheet.write_row(0, 0, df.columns)

            # Write data rows
            # iter_rows() is a memory-efficient way to loop through a Polars DataFrame
            for i, row in enumerate(df.iter_rows()):
                worksheet.write_row(i + 1, 0, row)