# run_benchmarks.py
"""
Automated performance and functional testing script for the SuperCourier ETL pipeline.

This script executes the main ETL pipeline with various data sizes and output formats,
capturing key performance metrics and logging them to a CSV file for analysis.
"""
import subprocess
import re
import csv
import os
import sys

# --- Path Configuration ---
# Make the script runnable from any directory by using absolute paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)  # Assumes tests/ is one level down from project root
MAIN_SCRIPT_PATH = os.path.join(PROJECT_ROOT, 'main.py')

# --- Test Configuration ---
# Larger test cases can take a significant amount of time.
test_cases = [
    {'rows': 1000, 'days': 90},
    {'rows': 5000, 'days': 450},
    {'rows': 10000, 'days': 900},
    {'rows': 100000, 'days': 9000},
    {'rows': 1000000, 'days': 90000}
]

output_choices = [
    {'choice': '6', 'formats': 'No xlsx'},
    {'choice': '7', 'formats': 'All'}
]

file_management_choice = '3'  # Delete ALL files for a clean slate

results_file = os.path.join(SCRIPT_DIR, 'benchmark_results.csv')
results_headers = ['Test Case', 'Deliveries', 'Weather Days', 'Output Format', 'Execution Time (s)', 'Status', 'Notes']

def run_test(rows, days, output_choice, file_mgmt_choice, timeout_seconds=600):
    """
    Executes the main ETL script with specific parameters and captures the output.
    Returns a tuple of (execution_time, status_message, full_captured_output).
    """
    # The inputs are passed as a single string with newlines
    inputs = f"{rows}\n{days}\n{file_mgmt_choice}\n{output_choice['choice']}\n"
    
    try:
        process = subprocess.run(
            [sys.executable, MAIN_SCRIPT_PATH],
            input=inputs,
            text=True,
            capture_output=True,
            check=False,
            cwd=PROJECT_ROOT,
            timeout=timeout_seconds
        )
    except subprocess.TimeoutExpired as e:
        # The process timed out. Capture any output it produced.
        full_output = e.stdout + "\n" + e.stderr if e.stdout and e.stderr else ""
        return None, "Timeout", full_output
    
    full_output = process.stdout + "\n" + process.stderr

    if process.returncode != 0:
        return None, f"Failed with exit code {process.returncode}", full_output
    
    time_pattern = r"ETL Pipeline completed successfully in (\d+(?:\.\d+)?) seconds"
    match = re.search(time_pattern, full_output)

    if match:
        execution_time = float(match.group(1))
        return execution_time, "Success", full_output
    else:
        # The script ran but the success message was not found in the output.
        return None, "Log pattern not found", full_output

def execute_benchmark_run(writer, case, choice):
    """
    Executes a single benchmark configuration, prints the output, and logs the result.
    """
    print("\n----------------------------------------------------------")
    print(f"Running test: {case['rows']} deliveries, {case['days']} days, format: {choice['formats']}")
    print("----------------------------------------------------------")
    
    # Set a dynamic timeout: 10 minutes for small tests, 30 for large ones.
    timeout = 1800 if case['rows'] >= 100000 else 600
    execution_time, status, output = run_test(case['rows'], case['days'], choice, file_management_choice, timeout_seconds=timeout)
    
    if output:
        print(output.strip())
    
    # Prepare the result row for the CSV.
    result_row = [
        f"{case['rows']}_deliveries_{choice['formats']}",
        case['rows'],
        case['days'],
        choice['formats'],
        f"{execution_time:.2f}" if execution_time is not None else "N/A",
        status,
        ""  # Placeholder for notes
    ]
    
    writer.writerow(result_row)

    # Print the summary to the console.
    if execution_time is not None:
        print(f"Test completed. Execution Time: {execution_time:.2f} seconds.")
    else:
        print(f"Test failed. Status: {status}.")

def main():
    """
    Main function to orchestrate the benchmark suite.
    """
    print("==========================================================")
    print("--- Starting SuperCourier ETL Benchmark Suite ---")
    print("==========================================================")

    try:
        with open(results_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(results_headers)

            # Iterate through all combinations of test cases and output formats.
            for case in test_cases:
                for choice in output_choices:
                    execute_benchmark_run(writer, case, choice)
                    f.flush()  # Ensure data is written to disk immediately after each test.

    except KeyboardInterrupt:
        print("\n\nBenchmark suite interrupted by user.")
    finally:
        print("\n==========================================================")
        print(f"--- Benchmark complete. Results saved to '{results_file}' ---")
        print("==========================================================")

if __name__ == "__main__":
    main()