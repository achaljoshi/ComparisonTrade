from fastapi import FastAPI, File, UploadFile, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from typing import Dict, Optional, List
from pydantic import BaseModel
from datetime import datetime
import pandas as pd
import json
from pathlib import Path
import tempfile
import os
import shutil
from utils.data_processor import DataProcessor, ComparisonResult

app = FastAPI(
    title="Discrepancy Analysis API",
    description="API for analyzing discrepancies between baseline and candidate files",
    version="1.0.0"
)

class ComparisonConfig(BaseModel):
    baseline_env: str
    baseline_label: str
    candidate_env: str
    candidate_label: str
    file_type: str
    filters: Optional[Dict] = None

class ComparisonResponse(BaseModel):
    job_id: str
    status: str
    timestamp: datetime
    results: Optional[Dict] = None
    error: Optional[str] = None

@app.post("/api/v1/compare")
async def compare_files(
    config: ComparisonConfig,
    background_tasks: BackgroundTasks,
    directory_config: UploadFile = File(...),
    job_response: UploadFile = File(...),
    rules_config: UploadFile = File(...),
    baseline_file: UploadFile = File(...),
    candidate_file: UploadFile = File(...)
) -> ComparisonResponse:
    job_id = f"job_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    try:
        processor = DataProcessor(
            json.load(directory_config.file),
            json.load(job_response.file),
            json.load(rules_config.file)
        )
        
        df_baseline = read_file_content(baseline_file, config.file_type)
        df_candidate = read_file_content(candidate_file, config.file_type)
        
        results = processor.compare_files(
            df_baseline,
            df_candidate,
            config.file_type,
            config.filters
        )
        
        return ComparisonResponse(
            job_id=job_id,
            status="completed",
            timestamp=datetime.now(),
            results=results.discrepancies.to_dict(orient="records")
        )
        
    except Exception as e:
        return ComparisonResponse(
            job_id=job_id,
            status="failed",
            timestamp=datetime.now(),
            error=str(e)
        )

def save_config_files(temp_dir: Path, *config_files: UploadFile) -> Dict[str, Path]:
    config_paths = {}
    for config_file in config_files:
        file_path = temp_dir / config_file.filename
        with open(file_path, "wb") as f:
            shutil.copyfileobj(config_file.file, f)
        config_paths[config_file.filename.replace(".json", "")] = file_path
    return config_paths

def save_data_files(temp_dir: Path, baseline_file: UploadFile, candidate_file: UploadFile) -> Dict[str, Path]:
    data_paths = {}
    for file_name, file in [("baseline", baseline_file), ("candidate", candidate_file)]:
        file_path = temp_dir / file.filename
        with open(file_path, "wb") as f:
            shutil.copyfileobj(file.file, f)
        data_paths[file_name] = file_path
    return data_paths

def read_file(file_path: Path, file_type: str) -> pd.DataFrame:
    if file_type == "Excel":
        return pd.read_excel(file_path)
    return pd.read_csv(file_path)

def format_comparison_results(results: ComparisonResult, config: ComparisonConfig) -> Dict:
    return {
        "metadata": {
            "baseline_env": config.baseline_env,
            "baseline_label": config.baseline_label,
            "candidate_env": config.candidate_env,
            "candidate_label": config.candidate_label,
            "file_type": config.file_type,
            "comparison_timestamp": datetime.now().isoformat()
        },
        "summary": {
            "total_discrepancies": len(results.discrepancies),
            "categories": results.discrepancies["Category"].value_counts().to_dict(),
            "missing_records": {
                "baseline": (results.discrepancies["Rule Type"] == "Missing in Baseline").sum(),
                "candidate": (results.discrepancies["Rule Type"] == "Missing in Candidate").sum()
            }
        },
        "discrepancies": results.discrepancies.to_dict(orient="records")
    }

def cleanup_temp_files(temp_dir: Path) -> None:
    shutil.rmtree(temp_dir)

@app.get("/api/v1/health")
async def health_check() -> Dict:
    return {
        "status": "healthy",
        "version": "1.0.0",
        "timestamp": datetime.now().isoformat()
    }

def read_file_content(file: UploadFile, file_type: str) -> pd.DataFrame:
    # Create a temporary file to store the uploaded content
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        # Write the uploaded file content to temp file
        shutil.copyfileobj(file.file, temp_file)
        temp_file_path = temp_file.name

    try:
        # Read the file based on type
        if file_type == "Excel":
            df = pd.read_excel(temp_file_path)
        else:
            df = pd.read_csv(temp_file_path)
        
        return df
    finally:
        # Clean up the temporary file
        os.unlink(temp_file_path)
