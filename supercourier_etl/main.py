# supercourier_etl/main.py
"""

"""
from typing import Optional
import typer
from dotenv import load_dotenv
from typing_extensions import Annotated
from .pipeline import Pipeline

load_dotenv()
app = typer.Typer()

def _interactive_wizard() -> dict:
    """Guides the user through a basic interactive session to build the config."""
    print("\n--- SuperCourier ETL Interactive Mode ---")

    # 1. Ask for source
    while True:
        choice = input("Choose a data source (1: Generate new data, 2: Use an existing file): ")
        if choice in ["1", "2"]:
            break
        print("Invalid input. Please enter 1 or 2.")

    source_config = {}
    if choice == "1":
        while True:
            rows_str = input("How many rows to generate? [1000]: ") or "1000"
            if rows_str.isdigit() and int(rows_str) > 0:
                source_config = {"type": "generate", "rows": int(rows_str)}
                break
            print("Invalid input. Please enter a positive number.")
    else:
        path = input("Enter the path to the source file: ")
        source_config = {"type": "file", "path": path}

    # 2. Ask for output format
    formats = ["preview", "csv", "json", "db", "parquet", "xlsx", "all", "all_but_xlsx"]
    while True:
        print("\nAvailable output formats:")
        for i, fmt in enumerate(formats):
            print(f"  {i+1}: {fmt}")
        choice_str = input("Choose an output format [1-8, default: preview]: ") or "1"
        if choice_str.isdigit() and 1 <= int(choice_str) <= len(formats):
            output_format = formats[int(choice_str) - 1]
            break
        print("Invalid input.")

    # 3. Ask for output path
    output_path = "output/deliveries"
    if output_format != "preview":
        path_input = input(f"Enter the base path for output files [default: {output_path}]: ")
        if path_input:
            output_path = path_input

    return {
        "source": source_config,
        "output": {"path": output_path, "format": output_format}
    }

@app.command()
def run_pipeline(
    generate_rows: Annotated[Optional[int], typer.Option(help="Number of rows to generate.")] = None,
    source_file: Annotated[Optional[str], typer.Option(help="Path to a source file to process.")] = None,
    output_path: Annotated[str, typer.Option(help="Base path for output files (without extension).")] = "output/deliveries",
    output_format: Annotated[str, typer.Option(help="Output format.")] = "preview"
):
    """Initializes and runs the complete ETL pipeline."""
    if generate_rows is not None and source_file is not None:
        print("Error: Cannot use --generate-rows and --source-file at the same time.")
        raise typer.Exit(code=1)

    config = {}
    if generate_rows or source_file:
        config["output"] = {"path": output_path, "format": output_format}
        if generate_rows:
            config["source"] = {"type": "generate", "rows": generate_rows}
        else:
            config["source"] = {"type": "file", "path": source_file}
    else:
        config = _interactive_wizard()

    pipeline = Pipeline(config)
    pipeline.run()

if __name__ == "__main__":
    app()
