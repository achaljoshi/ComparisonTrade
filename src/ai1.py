import pandas as pd
import json
import argparse
import sqlite3
import os
import uuid
from typing import Dict, Any
from fastapi import FastAPI, File, UploadFile, HTTPException
import threading
import dask.dataframe as dd
from openai import OpenAI
from flask import Flask, render_template, request, jsonify
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import xml.etree.ElementTree as ET

# Environment Variables and Configurations
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
UPLOAD_FOLDER = "uploads"
RESULTS_FOLDER = "results"
JOBS_FOLDER = "jobs"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(RESULTS_FOLDER, exist_ok=True)
os.makedirs(JOBS_FOLDER, exist_ok=True)

# FastAPI for API Management
app = FastAPI()
comparisons = {}
validation_rules = {}

# Flask App for Web UI Dashboard
flask_app = Flask(__name__)
app_dash = dash.Dash(__name__, server=flask_app, url_base_pathname="/dashboard/")

app_dash.layout = html.Div([
    html.H1("AI-Powered Discrepancy Detection Dashboard"),
    dcc.Interval(id='interval-component', interval=5000, n_intervals=0),
    html.Div(id='live-update-text')
])

@app_dash.callback(Output('live-update-text', 'children'), [Input('interval-component', 'n_intervals')])
def update_metrics(n):
    return json.dumps(comparisons, indent=4) if comparisons else "No active jobs."

def detect_column_types(df):
    """Detect column types dynamically based on dataset."""
    column_types = {}
    for column in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[column]):
            column_types[column] = "date"
        elif pd.api.types.is_numeric_dtype(df[column]):
            column_types[column] = "numeric"
        else:
            column_types[column] = "string"
    return column_types

def generate_ai_rules(baseline_file, candidate_file):
    """Generate rules_config.json dynamically using AI and observed differences."""
    df_baseline = load_file(baseline_file).compute()
    df_candidate = load_file(candidate_file).compute()
    column_types = detect_column_types(df_baseline)
    dynamic_thresholds = calculate_dynamic_thresholds(df_baseline, df_candidate)
    
    prompt = f"""
    Given the dataset columns and their types: {column_types}, and the dynamically observed differences: {dynamic_thresholds}, generate validation rules dynamically.
    Output rules in JSON format.
    """
    
    client = OpenAI(api_key=OPENAI_API_KEY)
    response = client.Completion.create(model="gpt-4", prompt=prompt, max_tokens=800)
    rules_config = json.loads(response['choices'][0]['text'])
    
    with open("rules_config.json", "w") as f:
        json.dump(rules_config, f, indent=4)
    
    return rules_config

@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    file_path = os.path.join(UPLOAD_FOLDER, file.filename)
    with open(file_path, "wb") as buffer:
        buffer.write(file.file.read())
    file_id = str(uuid.uuid4())
    comparisons[file_id] = file_path
    return {"file_id": file_id, "filename": file.filename}

@app.post("/compare")
async def compare_files(baseline_id: str, candidate_id: str):
    if baseline_id not in comparisons or candidate_id not in comparisons:
        raise HTTPException(status_code=404, detail="One or both file IDs not found.")
    
    result_id = str(uuid.uuid4())
    comparisons[result_id] = {"status": "processing", "baseline_id": baseline_id, "candidate_id": candidate_id}
    
    def run_comparison():
        rules_config = generate_ai_rules(comparisons[baseline_id], comparisons[candidate_id])
        comparisons[result_id]["status"] = "completed"
        comparisons[result_id]["results"] = rules_config
        with open(os.path.join(RESULTS_FOLDER, f"{result_id}.json"), "w") as f:
            json.dump(rules_config, f, indent=4)
    
    thread = threading.Thread(target=run_comparison)
    thread.start()
    
    return {"comparison_id": result_id, "message": "Comparison started, fetch results later."}

@app.get("/compare-job-id/{comparison_id}")
async def get_comparison_status(comparison_id: str):
    if comparison_id not in comparisons:
        raise HTTPException(status_code=404, detail="Comparison ID not found.")
    return {"comparison_id": comparison_id, "status": comparisons[comparison_id].get("status", "processing"), "results": comparisons[comparison_id].get("results")}

# CLI Implementation
def main():
    parser = argparse.ArgumentParser(description="ComparisonTrade AI Tool")
    parser.add_argument("--generate-rules", nargs=2, metavar=('BASELINE', 'CANDIDATE'), help="Generate rules_config.json dynamically")
    parser.add_argument("--compare", nargs=2, metavar=('BASELINE', 'CANDIDATE'), help="Compare datasets using AI-generated rules")
    args = parser.parse_args()
    
    if args.generate_rules:
        generate_ai_rules(args.generate_rules[0], args.generate_rules[1])
    elif args.compare:
        job_id = str(uuid.uuid4())
        comparisons[job_id] = generate_ai_rules(args.compare[0], args.compare[1])
        print(f"Comparison started with Job ID: {job_id}")
    else:
        parser.print_help()
    
if __name__ == "__main__":
    main()
