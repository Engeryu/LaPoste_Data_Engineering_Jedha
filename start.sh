#!/bin/bash

set -e
echo "--- Listing all files in /app directory ---"
ls -R /app
echo "--- End of file list ---"
conda run -n supercourier-etl uvicorn api:app --host 0.0.0.0 --port 80