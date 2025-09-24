# api.py
"""
Main web application powered by FastAPI.
"""

import os
import uuid
import json
import shutil
import zipfile
import traceback
from pathlib import Path
from typing import Dict, Any, List

from fastapi import FastAPI, Request, File, UploadFile, Form, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from supercourier_etl.pipeline import Pipeline

# --- App Initialization ---
app = FastAPI(title="SuperCourier ETL API")

# --- Global State & Configuration ---
JOBS: Dict[str, Dict[str, Any]] = {}

# Define base directories
BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
TEMPLATES_DIR = BASE_DIR / "templates"
OUTPUT_DIR = BASE_DIR / "output_runs"

os.makedirs(OUTPUT_DIR, exist_ok=True)

app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
templates = Jinja2Templates(directory=TEMPLATES_DIR)

# --- Background Task Definition ---

def run_etl_task(
    config: dict,
    session_id: str,
    temp_file_path: str | None = None
):
    """
    The actual ETL process function that runs in the background.
    It updates the job status in the global JOBS dictionary.
    """
    session_dir = OUTPUT_DIR / session_id
    try:
        os.makedirs(session_dir, exist_ok=True)

        config["output"]["path"] = str(session_dir / "deliveries")

        pipeline = Pipeline(config)
        pipeline.run()

        # --- Package results into a zip file ---
        zip_path = OUTPUT_DIR / f"{session_id}.zip"
        with zipfile.ZipFile(zip_path, 'w') as zipf:
            for file in session_dir.glob('*'):
                zipf.write(file, file.name)

        JOBS[session_id] = {
            "status": "completed",
            "download_url": f"/download/{session_id}"
        }

    except Exception as e:
        JOBS[session_id] = {"status": "error", "message": str(e)}

    finally:
        # --- Cleanup ---
        if temp_file_path and os.path.exists(temp_file_path):
            os.remove(temp_file_path)
        if os.path.exists(session_dir):
            shutil.rmtree(session_dir)


# --- API Endpoints ---
@app.get("/", response_class=HTMLResponse)
async def get_index(request: Request):
    """
    Endpoint de débogage pour capturer l'erreur de rendu du template.
    """
    try:
        # On tente l'opération qui échoue
        return templates.TemplateResponse("index.html", {"request": request})
    except Exception as e:
        # Si ça échoue, on retourne la trace d'erreur complète en JSON
        return JSONResponse(
            status_code=500,
            content={
                "error_type": str(type(e)),
                "traceback": traceback.format_exc(),
            }
        )

@app.post("/run-etl")
async def start_etl_run(
    background_tasks: BackgroundTasks,
    rows: int = Form(None),
    formats: str = Form(...),
    file: UploadFile = File(None),
):
    """
    Starts an ETL job in the background and returns a session ID.
    """
    session_id = str(uuid.uuid4())
    JOBS[session_id] = {"status": "processing"}

    config = {"output": {"format": "all"}}
    temp_file_to_clean = None

    if file:
        temp_dir = OUTPUT_DIR / "_temp"
        os.makedirs(temp_dir, exist_ok=True)
        temp_file_path = temp_dir / file.filename
        with open(temp_file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        config["source"] = {"type": "file", "path": str(temp_file_path)}
        temp_file_to_clean = temp_file_path

    elif rows:
        config["source"] = {"type": "generate", "rows": rows}
    else:
        return JSONResponse(status_code=400, content={"error": "No source provided (file or rows)."})

    selected_formats = json.loads(formats)
    if not selected_formats:
         return JSONResponse(status_code=400, content={"error": "At least one format must be selected."})

    config["output"]["format"] = "all"

    background_tasks.add_task(run_etl_task, config, session_id, temp_file_to_clean)

    return JSONResponse({"session_id": session_id})

@app.get("/status/{session_id}")
async def get_job_status(session_id: str):
    """Pollable endpoint to check the status of a job."""
    job = JOBS.get(session_id)
    if not job:
        return JSONResponse(status_code=404, content={"error": "Job not found."})
    return JSONResponse(job)

@app.get("/download/{session_id}")
async def download_results(session_id: str):
    """Provides the final ZIP file for download."""
    zip_path = OUTPUT_DIR / f"{session_id}.zip"
    if not zip_path.exists():
        return JSONResponse(status_code=404, content={"error": "File not found."})

    return FileResponse(
        path=zip_path,
        media_type='application/zip',
        filename=f"supercourier_results_{session_id}.zip"
    )
