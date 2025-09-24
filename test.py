# main_api.py
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
import logging
import pathlib
from fastapi import FastAPI, File, UploadFile, Form, BackgroundTasks, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from dotenv import load_dotenv

load_dotenv()
BASE_DIR = pathlib.Path(__file__).parent.resolve()

app = FastAPI(
    title="SuperCourier ETL API",
    description="API to run the delivery data enrichment pipeline.",
    version="1.0.0"
)

app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
templates = Jinja2Templates(directory=BASE_DIR / "templates")

os.makedirs("temp_uploads", exist_ok=True)
os.makedirs("temp_results", exist_ok=True)

def run_pipeline_task(config: dict, session_id: str):
    """
    Executes the ETL pipeline in the background and zips the results.
    The heavy 'Pipeline' import is done here to prevent startup hangs.
    
    Args:
        config (dict): The configuration dictionary for the pipeline.
        session_id (str): The unique ID for the current session.
    """
    from supercourier_etl.pipeline import Pipeline

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
        logging.error(f"ETL task failed for session {session_id}", exc_info=True)
        error_file_path = f"temp_results/{session_id}.error"
        with open(error_file_path, "w", encoding="utf-8") as f:
            f.write(str(e))
    finally:
        source_dir = os.path.dirname(config.get("source", {}).get("path", ""))
        output_dir_to_clean = os.path.dirname(config.get("output", {}).get("path", ""))

        if source_dir and os.path.exists(source_dir):
            shutil.rmtree(source_dir)
        if output_dir_to_clean and os.path.exists(output_dir_to_clean):
            shutil.rmtree(output_dir_to_clean)


@app.get("/", response_class=HTMLResponse, tags=["UI"])
async def get_index(request: Request):
    """
    Serves the main HTML user interface.
    """
    return templates.TemplateResponse("index.html", {"request": request})

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
    """
    session_id = str(uuid.uuid4())
    upload_dir = f"temp_uploads/{session_id}"
    output_base_path = f"{upload_dir}/results"
    
    output_formats = json.loads(formats)
    output_format_str = "all" if len(output_formats) > 1 else output_formats[0]

    config = { "output": {"path": output_base_path, "format": output_format_str} }

    if file and file.filename:
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
    """
    zip_path = f"temp_results/{session_id}.zip"
    error_path = f"temp_results/{session_id}.error"
    if os.path.exists(zip_path):
        return {"status": "completed", "download_url": f"/download/{session_id}"}
    if os.path.exists(error_path):
        with open(error_path, "r", encoding="utf-8") as f:
            return {"status": "error", "message": f.read()}
    return {"status": "processing"}

@app.get("/download/{session_id}", response_class=FileResponse, tags=["ETL"])
async def download_results(session_id: str):
    """
    Serves the final ZIP archive for download.
    """
    path = f"temp_results/{session_id}.zip"
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="File not found.")
    return FileResponse(path, media_type='application/zip', filename=f'supercourier_results_{session_id}.zip')