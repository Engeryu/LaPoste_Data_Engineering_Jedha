# supercourier_etl/main.py
import typer
from dotenv import load_dotenv
from .pipeline import Pipeline

# Load environment variables from .env file
load_dotenv()

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