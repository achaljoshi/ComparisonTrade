import pandas as pd
import json
import argparse
import sqlite3
import os
import uuid
import threading
import dask.dataframe as dd
import uvicorn
import streamlit as st
import plotly.express as px
from fastapi import FastAPI, File, UploadFile, HTTPException
from openai import OpenAI
import time
import random


# Environment Variables and Configurations
UPLOAD_FOLDER = "uploads"
RESULTS_FOLDER = "results"
JOBS_FOLDER = "jobs"
JOBS_FILE = "jobs.json"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(RESULTS_FOLDER, exist_ok=True)
os.makedirs(JOBS_FOLDER, exist_ok=True)

# FastAPI for API Management
app = FastAPI()
comparisons = {}

def run_comparison(baseline_file, candidate_file, job_id):
    """ Compares two datasets and stores full discrepancies """
    comparisons[job_id] = {"status": "processing", "baseline": baseline_file, "candidate": candidate_file}
    save_jobs()  # ✅ Save the job status

    # Simulating actual file comparison
    df_baseline = pd.read_csv(os.path.join(UPLOAD_FOLDER, baseline_file))
    df_candidate = pd.read_csv(os.path.join(UPLOAD_FOLDER, candidate_file))

    # Simulating some mismatches
    discrepancies = []
    for i in range(len(df_baseline)):
        if i % 5 == 0:  # Simulated mismatches every 5th row
            discrepancy = {
                "row": i,
                "column": random.choice(df_baseline.columns),
                "baseline_value": random.randint(100, 500),
                "candidate_value": random.randint(100, 500),
                "severity": random.choice(["INFO", "WARNING", "FATAL"])
            }
            discrepancies.append(discrepancy)

    # Update results after processing
    comparisons[job_id]["status"] = "completed"
    comparisons[job_id]["results"] = {
        "mismatch_count": len(discrepancies),
        "critical_errors": sum(1 for d in discrepancies if d["severity"] == "FATAL"),
        "data": discrepancies
    }
    save_jobs()  # ✅ Save the final status

# ✅ Streamlit UI Function
def streamlit_ui():
    st.set_page_config(page_title="Discrepancy Detection", layout="wide")
    st.title("🔍 AI-Powered Discrepancy Detection Dashboard")

    # File Upload Section
    uploaded_files = st.file_uploader("📂 Upload Files for Comparison", accept_multiple_files=True, type=["csv", "xlsx", "json"])
    if uploaded_files:
        filenames = []
        for uploaded_file in uploaded_files:
            file_path = os.path.join(UPLOAD_FOLDER, uploaded_file.name)
            with open(file_path, "wb") as f:
                f.write(uploaded_file.getbuffer())
            filenames.append(uploaded_file.name)

        st.success(f"✅ Uploaded files: {', '.join(filenames)}")

        # Start Comparison Button
        if st.button("🚀 Start Comparison"):
            if len(filenames) >= 2:
                job_id = str(uuid.uuid4())
                baseline_file, candidate_file = filenames[:2]
                comparisons[job_id] = {"status": "processing", "baseline": baseline_file, "candidate": candidate_file}

                # Run comparison in a separate thread
                thread = threading.Thread(target=run_comparison, args=(baseline_file, candidate_file, job_id))
                thread.start()

                st.write(f"✅ Comparison started with Job ID: {job_id}. Check results below ⬇️")
            else:
                st.warning("⚠️ Please upload at least two files for comparison.")

    # Section to Check Job Status
    st.subheader("📊 Check Comparison Results")
    job_id_input = st.text_input("🔍 Enter Job ID to View Results")
    if st.button("📥 Fetch Results"):
        if job_id_input in comparisons:
            status = comparisons[job_id_input]
            st.json(status)

            # Display KPIs
            if status["status"] == "completed":
                results = status["results"]
                mismatch_count = results["mismatch_count"]
                critical_errors = results["critical_errors"]
                discrepancy_data = results["data"]

                # KPI Metrics
                col1, col2 = st.columns(2)
                col1.metric(label="⚡ Total Mismatches", value=mismatch_count)
                col2.metric(label="🔥 Critical Errors", value=critical_errors)

                # Discrepancy Table with Color Coding
                st.subheader("📋 Detailed Discrepancy Report")
                df = pd.DataFrame(discrepancy_data)
                df["color"] = df["severity"].map({"INFO": "🔵", "WARNING": "🟡", "FATAL": "🔴"})
                st.dataframe(df[["row", "column", "baseline_value", "candidate_value", "severity", "color"]])

                # Bar Chart for Mismatches by Severity
                st.subheader("📊 Mismatch Severity Breakdown")
                fig = px.bar(df, x="severity", title="Mismatch Severity Distribution", color="severity", text_auto=True)
                st.plotly_chart(fig, use_container_width=True)

                # Pie Chart for Severity Distribution
                st.subheader("🍰 Severity Distribution")
                fig_pie = px.pie(df, names="severity", title="Severity Breakdown", hole=0.3)
                st.plotly_chart(fig_pie, use_container_width=True)
        else:
            st.warning("⚠️ Job ID not found.")

# ✅ FastAPI Endpoint for Uploading Files via API
@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    file_path = os.path.join(UPLOAD_FOLDER, file.filename)
    with open(file_path, "wb") as buffer:
        buffer.write(file.file.read())
    file_id = str(uuid.uuid4())
    comparisons[file_id] = file_path
    return {"file_id": file_id, "filename": file.filename}

# ✅ FastAPI Endpoint to Start a Comparison via API
@app.post("/compare")
async def compare_files(baseline_id: str, candidate_id: str):
    if baseline_id not in comparisons or candidate_id not in comparisons:
        raise HTTPException(status_code=404, detail="One or both file IDs not found.")
    
    job_id = str(uuid.uuid4())
    comparisons[job_id] = {"status": "processing", "baseline": baseline_id, "candidate": candidate_id}
    
    def run_comparison():
        # Simulate comparison logic (Replace with real logic)
        import time
        time.sleep(5)  # Simulating processing time
        comparisons[job_id]["status"] = "completed"
        comparisons[job_id]["results"] = {"mismatch_count": 10, "critical_errors": 2}

    thread = threading.Thread(target=run_comparison)
    thread.start()

    return {"comparison_id": job_id, "message": "Comparison started, fetch results later."}

# ✅ Load Existing Jobs from File
def load_jobs():
    if os.path.exists(JOBS_FILE):
        with open(JOBS_FILE, "r") as f:
            return json.load(f)
    return {}

# ✅ Save Jobs to File
def save_jobs():
    with open(JOBS_FILE, "w") as f:
        json.dump(comparisons, f, indent=4)

comparisons = load_jobs()  # ✅ Load existing jobs on startup

# ✅ API to Check Job Status
@app.get("/compare-job-id/{comparison_id}")
async def get_comparison_status(comparison_id: str):
    comparisons = load_jobs()  # ✅ Ensure latest jobs are loaded
    if comparison_id not in comparisons:
        raise HTTPException(status_code=404, detail="Comparison ID not found.")
    return {"comparison_id": comparison_id, "status": comparisons[comparison_id]["status"], "results": comparisons[comparison_id].get("results")}

# ✅ Run FastAPI & Streamlit UI in Separate Processes
if __name__ == "__main__":
    fastapi_process = threading.Thread(target=lambda: uvicorn.run(app, host="0.0.0.0", port=5050, reload=False), daemon=True)
    fastapi_process.start()

    # Start Streamlit UI
    streamlit_ui()
