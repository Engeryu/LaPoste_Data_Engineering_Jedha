# supercourier_etl/pipeline.py
"""
    Final Pipeline to orchestrates main functions of the Core Folder.
"""
import os
import time
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn
from .core.extract import Extractor
from .core.transform import Transformer
from .core.load import Loader

class Pipeline:
    """Orchestrates the entire ETL process from extraction to loading."""

    def __init__(self, config: dict):
        self.config = config
        self.extractor = Extractor(config)
        self.transformer = Transformer(config)
        self.loader = Loader(config)

    def run(self) -> float:
        """Executes the ETL pipeline steps in order with a rich progress bar."""
        start_time = time.perf_counter()

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeElapsedColumn()
        ) as progress:
            etl_task = progress.add_task("[bold blue]Overall ETL Progress", total=3)

            # --- 1. Extraction (avec description dynamique) ---
            source_type = self.config.get("source", {}).get("type", "generate")
            if source_type == 'generate':
                rows = self.config["source"]["rows"]
                extract_task_desc = f"[green]Generating {rows:,} records..."
                extract_total = rows
            else:
                path = self.config["source"]["path"]
                extract_task_desc = f"[green]Reading file {os.path.basename(path)}..."
                extract_total = 1

            extract_task = progress.add_task(extract_task_desc, total=extract_total)
            extracted_data = self.extractor.extract_data(progress, extract_task)
            progress.update(extract_task, completed=extract_total, description=f"[green]Extraction complete ({len(extracted_data):,} records)")
            progress.advance(etl_task)

            # --- 2. Transformation (maintenant un conteneur pour sous-tâches) ---
            transform_task = progress.add_task("[magenta]Applying transformations...", total=1)
            transformed_data = self.transformer.transform_data(extracted_data, progress, transform_task)
            progress.update(transform_task, description="[magenta]Transformation complete")

            # --- 3. Loading (géré par le Loader) ---
            self.loader.load_data(transformed_data, progress, etl_task)

        end_time = time.perf_counter()
        duration = end_time - start_time
        print("\n--- ETL process finished successfully ---")
        print(f"Total execution time: {duration:.2f} seconds.")

        return duration