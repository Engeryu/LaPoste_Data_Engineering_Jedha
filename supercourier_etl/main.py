# supercourier_etl/main.py
import typer
from dotenv import load_dotenv
from typing_extensions import Annotated
from typing import Optional
from .pipeline import Pipeline

load_dotenv()
app = typer.Typer()

@app.command()
def run_pipeline(
    generate_rows: Annotated[Optional[int], typer.Option(help="Number of rows to generate.")] = None,
    source_file: Annotated[Optional[str], typer.Option(help="Path to a source file to process.")] = None,
    output_path: Annotated[str, typer.Option(help="Base path for output files (without extension).")] = "output/deliveries",
    output_format: Annotated[str, typer.Option(help="Output format: csv, json, parquet, xlsx, all, all_but_xlsx, or preview.")] = "preview"
):
    """
    Initializes and runs the complete ETL pipeline.

    This CLI command allows specifying the data source (generation or file)
    and the desired output format and location.
    """
    if generate_rows is not None and source_file is not None:
        print("Error: Cannot use --generate-rows and --source-file at the same time.")
        raise typer.Exit(code=1)

    config = {"output": {"path": output_path, "format": output_format}}

    if generate_rows:
        config["source"] = {"type": "generate", "rows": generate_rows}
    elif source_file:
        config["source"] = {"type": "file", "path": source_file}
    else:
        print("Defaulting to generating 1000 rows.")
        config["source"] = {"type": "generate", "rows": 1000}

    pipeline = Pipeline(config)
    pipeline.run()

if __name__ == "__main__":
    app()