import os
import tempfile
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import JSONResponse, FileResponse
import pandas as pd
from utils.data_processor import DataProcessor

# ✅ Ensure output directory exists
OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ✅ MIME types for different file formats
MIME_TYPES = {
    "csv": "text/csv",
    "json": "application/json",
    "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "txt": "text/plain",
}

app = FastAPI()

@app.post("/compare/")
async def compare_files(
    baseline_file: UploadFile = File(...),
    candidate_file: UploadFile = File(...),
    directory_config: UploadFile = File(...),
    job_response: UploadFile = File(...),
    rules_config: UploadFile = File(...),
    file_type: str = Form(...),
    output_format: str = Form("json")  # Supports: json, csv, xlsx, txt
):
    temp_files = []  # ✅ Store temp file paths for safe deletion

    try:
        # ✅ Save uploaded files to temporary files
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as temp_baseline:
            temp_baseline.write(await baseline_file.read())
            baseline_path = temp_baseline.name
            temp_files.append(baseline_path)

        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as temp_candidate:
            temp_candidate.write(await candidate_file.read())
            candidate_path = temp_candidate.name
            temp_files.append(candidate_path)

        with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as temp_directory:
            temp_directory.write(await directory_config.read())
            directory_path = temp_directory.name
            temp_files.append(directory_path)

        with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as temp_job:
            temp_job.write(await job_response.read())
            job_path = temp_job.name
            temp_files.append(job_path)

        with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as temp_rules:
            temp_rules.write(await rules_config.read())
            rules_path = temp_rules.name
            temp_files.append(rules_path)

        # ✅ Initialize DataProcessor
        tool = DataProcessor(directory_path, job_path, rules_path)

        # ✅ Read Excel files into DataFrames
        df_baseline = pd.read_excel(baseline_path, engine="openpyxl") if file_type == "Excel" else pd.read_csv(baseline_path)
        df_candidate = pd.read_excel(candidate_path, engine="openpyxl") if file_type == "Excel" else pd.read_csv(candidate_path)

        # ✅ Run Comparison
        results = tool.compare_files(df_baseline, df_candidate, file_type)

        # ✅ Save output to requested format
        output_filename = f"comparison_output.{output_format.lower()}"
        output_filepath = os.path.join(OUTPUT_DIR, output_filename)
        file_extension = output_format.lower()

        if file_extension == "csv":
            results.to_csv(output_filepath, index=False)

        elif file_extension == "json":
            results.to_json(output_filepath, orient="records", indent=2)

        elif file_extension in ["xlsx", "excel"]:
            results.to_excel(output_filepath, index=False, engine="openpyxl")

        elif file_extension in ["txt", "text"]:
            results_text = results.to_string(index=False)  # ✅ FIX: Properly format DataFrame as text
            with open(output_filepath, "w", encoding="utf-8") as f:
                f.write(results_text)

        else:
            raise HTTPException(status_code=400, detail="Unsupported output format. Use json, csv, xlsx, or txt.")

        # ✅ Cleanup Temporary Files (except output)
        for file_path in temp_files:
            try:
                os.remove(file_path)
            except Exception as e:
                print(f"Warning: Failed to delete temp file {file_path} - {str(e)}")

        # ✅ Return JSON response for non-file formats
        if file_extension == "json":
            return JSONResponse(
                content={
                    "message": "Comparison completed successfully.",
                    "output_format": "json",
                    "data": results.to_dict(orient="records")
                }
            )

        # ✅ Otherwise, return download URL
        return JSONResponse(
            content={
                "message": "Comparison completed successfully.",
                "output_format": file_extension,
                "download_url": f"/download/{output_filename}"
            }
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing files: {str(e)}")


@app.get("/download/{filename}")
async def download_output(filename: str):
    """Download the generated output file with correct MIME type."""
    filepath = os.path.join(OUTPUT_DIR, filename)

    # ✅ Ensure file exists
    if not os.path.exists(filepath):
        raise HTTPException(status_code=404, detail="File not found")

    # ✅ Determine MIME type from file extension
    file_extension = filename.split(".")[-1]
    mime_type = MIME_TYPES.get(file_extension, "application/octet-stream")

    return FileResponse(
        path=filepath,
        filename=filename,
        media_type=mime_type
    )
