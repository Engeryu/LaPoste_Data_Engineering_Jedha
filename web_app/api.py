# web_app/api.py
"""
This module defines the FastAPI web server for the SuperCourier ETL pipeline.

It provides endpoints to:
1. Serve the main HTML user interface.
2. Trigger the ETL pipeline with user-provided data (file upload or generation).
3. Poll for the status of a running ETL job.
4. Download the resulting ZIP archive.
"""

import os
import shutil
import uuid
import zipfile
import json
from typing import List
from fastapi import FastAPI, File, UploadFile, Form, BackgroundTasks, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field
from dotenv import load_dotenv

from supercourier_etl.pipeline import Pipeline

# Load environment variables for the application
load_dotenv()

# --- FastAPI App Setup ---
app = FastAPI(
    title="SuperCourier ETL API",
    description="API to run the delivery data enrichment pipeline.",
    version="1.0.0"
)
app.mount("/static", StaticFiles(directory="web_app/static"), name="static")
templates = Jinja2Templates(directory="web_app/templates")

# Create temporary directories for file handling
os.makedirs("temp_uploads", exist_ok=True)
os.makedirs("temp_results", exist_ok=True)


class EtlRequest(BaseModel):
    """Pydantic model for the ETL request body, providing data validation."""
    rows: int = Field(1000, gt=0, le=100000, description="Number of rows to generate.")
    formats: List[str] = Field(..., description="List of output formats.")
    file: UploadFile = None


def run_pipeline_task(config: dict, session_id: str):
    """
    Executes the ETL pipeline in the background and zips the results.

    Args:
        config (dict): The configuration dictionary for the pipeline.
        session_id (str): The unique ID for the current session.
    """
    try:
        pipeline = Pipeline(config)
        pipeline.run()

        output_path = config["output"]["path"]
        output_dir = os.path.dirname(output_path)
        zip_path = f"temp_results/{session_id}.zip"

        with zipfile.ZipFile(zip_path, 'w') as zipf:
            for root, _, files in os.walk(output_dir):
                for file in files:
                    zipf.write(os.path.join(root, file), arcname=file)

    except Exception as e:
        error_file_path = f"temp_results/{session_id}.error"
        with open(error_file_path, "w", encoding="utf-8") as f:
            f.write(f"An error occurred during processing: {str(e)}")
    finally:
        # Clean up temporary directories
        if "path" in config.get("source", {}):
            upload_dir = os.path.dirname(config["source"]["path"])
            if os.path.exists(upload_dir):
                shutil.rmtree(upload_dir)

        output_dir = os.path.dirname(config["output"]["path"])
        if os.path.exists(output_dir):
            shutil.rmtree(output_dir)


@app.get("/", response_class=HTMLResponse, tags=["UI"])
async def get_index(request: Request):
    """
    Serves the main HTML user interface.

    Args:
        request (Request): The incoming request object from FastAPI.

    Returns:
        TemplateResponse: The rendered index.html page.
    """
    return templates

@app.post("/run-etl", tags=["ETL"])
async def handle_etl_request(
    background_tasks: BackgroundTasks,
    formats: str = Form(...),
    file: UploadFile = File(None),
    rows: int = Form(1000)
):
    """
    Receives user request, validates it, and starts the ETL pipeline
    as a background task to avoid HTTP timeouts.

    Args:
        background_tasks (BackgroundTasks): FastAPI dependency to run tasks after responding.
        formats (str): A JSON string representing the list of desired output formats.
        file (UploadFile, optional): The user-uploaded data file. Defaults to None.
        rows (int, optional): The number of rows to generate if no file is provided.

    Returns:
        dict: A confirmation message and a unique session_id for status polling.
    """
    session_id = str(uuid.uuid4())
    output_path = f"temp_uploads/{session_id}/results"
    
    config = {
        "output": {"path": output_path, "format": json.loads(formats)}
    }

    if file and file.filename:
        upload_dir = os.path.dirname(output_path)
        os.makedirs(upload_dir, exist_ok=True)
        file_path = os.path.join(upload_dir, file.filename)

        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        config["source"] = {"type": "file", "path": file_path}
    else:
        config["source"] = {"type": "generate", "rows": rows}

    background_tasks.add_task(run_pipeline_task, config, session_id)

    return {"message": "Processing started.", "session_id": session_id}

@app.get("/status/{session_id}", tags=["ETL"])
async def check_status(session_id: str):
    """
    Allows the frontend to poll for the status of a background job.

    Args:
        session_id (str): The unique ID of the processing job.

    Returns:
        dict: The current status ('processing', 'completed', or 'error').
    """
    zip_path = f"temp_results/{session_id}.zip"
    error_path = f"temp_results/{session_id}.error"

    if os.path.exists(zip_path):
        return {"status": "completed", "download_url": f"/download/{session_id}"}
    elif os.path.exists(error_path):
        with open(error_path, "r") as f:
            error_message = f.read()
        return {"status": "error", "message": error_message}
    else:
        return {"status": "processing"}

@app.get("/download/{session_id}", response_class=FileResponse, tags=["ETL"])
async def download_results(session_id: str):
    """
    Serves the final ZIP archive for download.

    Args:
        session_id (str): The unique ID of the processing job.

    Returns:
        FileResponse: The ZIP file to be downloaded by the user.
    """
    path = f"temp_results/{session_id}.zip"
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="File not found or has expired.")

    return FileResponse(path, media_type='application/zip', filename=f'supercourier_results_{session_id}.zip')
