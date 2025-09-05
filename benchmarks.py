# benchmark.py
import os
import shutil
import polars as pl
from rich.console import Console
from rich.table import Table
from dotenv import load_dotenv

# Important d'importer Pipeline APRÈS load_dotenv pour que la clé API soit disponible
load_dotenv()
from supercourier_etl.pipeline import Pipeline

# Scénarios de test à exécuter
BENCHMARK_SCENARIOS = [
    # (nombre de lignes, format de sortie)
    (1000, "all_but_xlsx"),
    (1000, "all"),
    (10000, "all_but_xlsx"),
    (10000, "all"),
    (100000, "all_but_xlsx"),
    (100000, "all"),
    (1000000, "all_but_xlsx"),
    (1000000, "all"),
    (10000000, "all_but_xlsx"),
    (10000000, "all"),
]

def run_benchmarks():
    """
    Runs the ETL pipeline against a series of predefined scenarios
    and reports the performance metrics.
    """
    console = Console()
    results = []
    
    output_dir = "output_benchmark"
    
    console.print("[bold yellow]Starting ETL Benchmark...", justify="center")

    for i, (rows, output_format) in enumerate(BENCHMARK_SCENARIOS):
        scenario_name = f"Run {i+1}/{len(BENCHMARK_SCENARIOS)}: {rows:,} rows, format='{output_format}'"
        console.print(f"\n[bold cyan]Executing: {scenario_name}[/bold cyan]")
        
        # Supprime le dossier de sortie précédent pour un test propre
        if os.path.exists(output_dir):
            shutil.rmtree(output_dir)

        config = {
            "source": {"type": "generate", "rows": rows},
            "output": {"path": f"{output_dir}/results", "format": output_format}
        }

        pipeline = Pipeline(config)
        duration = pipeline.run()

        results.append({
            "rows": rows,
            "output_format": output_format,
            "duration_seconds": duration
        })

    # --- Affichage et sauvegarde des résultats ---
    console.print("\n[bold yellow]Benchmark Complete. Results:", justify="center")

    results_df = pl.DataFrame(results)

    # Créer une table `rich` pour un affichage propre dans la console
    table = Table(title="Benchmark Results")
    table.add_column("Rows", justify="right", style="cyan")
    table.add_column("Output Format", style="magenta")
    table.add_column("Duration (seconds)", justify="right", style="green")

    for row in results_df.iter_rows(named=True):
        table.add_row(f"{row['rows']:,}", row['output_format'], f"{row['duration_seconds']:.2f}")

    console.print(table)
    
    # Sauvegarder les résultats dans un fichier CSV
    results_csv_path = "benchmark_results.csv"
    results_df.write_csv(results_csv_path)
    console.print(f"\nResults also saved to [bold green]{results_csv_path}[/bold green]")
    
    # Nettoyage final
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    console.print("Cleaned up output directory.")


if __name__ == "__main__":
    run_benchmarks()