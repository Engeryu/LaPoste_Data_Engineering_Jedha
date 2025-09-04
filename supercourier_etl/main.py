# supercourier_etl/main.py
import typer
from .pipeline import Pipeline

app = typer.Typer()

@app.command()
def run_pipeline():
    """
    Initializes and runs the ETL pipeline with a sample configuration.
    """
    # For now, we use a hardcoded configuration for testing generation
    config = {
        "generate": {
            "rows": 1000
        },
        "output": {
            "format": "preview" # 'preview' will just print to console
        }
    }
    
    pipeline = Pipeline(config)
    pipeline.run()

if __name__ == "__main__":
    app()