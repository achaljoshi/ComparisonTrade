import pandas as pd
import json
import uuid
import argparse
import sys
import os
import time
from openai import OpenAI
from flask import Flask, request, jsonify
from werkzeug.utils import secure_filename

# OpenAI API Key (Use environment variable instead of hardcoding)
OPENAI_API_KEY = "your_openai_api_key"
UPLOAD_FOLDER = "uploads"
RESULTS_FOLDER = "results"
JOBS_FOLDER = "jobs"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(RESULTS_FOLDER, exist_ok=True)
os.makedirs(JOBS_FOLDER, exist_ok=True)

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['RESULTS_FOLDER'] = RESULTS_FOLDER
app.config['JOBS_FOLDER'] = JOBS_FOLDER

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

def read_file(file_path, file_type):
    """Read file based on its type including Text, CSV, Excel."""
    if file_type == "Text":
        with open("rules_config.json", "r") as f:
            rules_config = json.load(f)
        delimiter = rules_config.get("text_file_delimiter", ",")
        header = 0 if rules_config.get("text_file_contains_header", "yes").lower() == "yes" else None
        return pd.read_csv(file_path, delimiter=delimiter, header=header)
    elif file_type == "CSV":
        return pd.read_csv(file_path)
    elif file_type == "Excel":
        return pd.read_excel(file_path)
    else:
        raise ValueError("Unsupported file type")

def calculate_dynamic_thresholds(df_baseline, df_candidate):
    """Calculate dynamic thresholds for large dataset comparisons."""
    thresholds = {}
    for column in df_baseline.columns:
        if column in df_candidate.columns and pd.api.types.is_numeric_dtype(df_baseline[column]):
            differences = (df_baseline[column] - df_candidate[column]).abs()
            thresholds[column] = {
                "acceptable_threshold": differences.mean(),
                "warning_threshold": {"min": differences.mean(), "max": differences.mean() + 2 * differences.std()},
                "fatal_threshold": {"min": differences.mean() + 2 * differences.std()}
            }
    return thresholds

def generate_ai_rules(baseline_file, candidate_file):
    """Generate rules_config.json dynamically using AI and observed differences."""
    
    df_baseline = read_file(baseline_file, "CSV")
    df_candidate = read_file(candidate_file, "CSV")
    
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

@app.route('/compare', methods=['POST'])
def compare_files():
    data = request.json
    job_id = str(uuid.uuid4())
    job_path = os.path.join(app.config['JOBS_FOLDER'], f"{job_id}.json")
    
    df_baseline = read_file(data.get("df_baseline"), data.get("file_type"))
    df_candidate = read_file(data.get("df_candidate"), data.get("file_type"))
    
    with open("rules_config.json", "r") as f:
        rules_config = json.load(f)
    
    comparison_results = {}
    for rule in rules_config["rules"]:
        column = rule["columns"]
        rule_type = rule["type"]
        
        if rule_type == "tolerance_check":
            threshold = rule["acceptable_threshold"]
            comparison_results[column] = (df_baseline[column] - df_candidate[column]).abs() <= threshold
        elif rule_type == "String_Check":
            comparison_results[column] = df_baseline[column].str.casefold() == df_candidate[column].str.casefold()
        elif rule_type == "Date_check":
            threshold = pd.Timedelta(days=rule.get("days_threshold", 1))
            comparison_results[column] = (pd.to_datetime(df_baseline[column]) - pd.to_datetime(df_candidate[column])).abs() <= threshold
    
    with open(job_path, "w") as f:
        json.dump(comparison_results, f, indent=4)
    
    return jsonify({"job_id": job_id, "message": "Comparison started successfully"})

@app.route('/job_status/<job_id>', methods=['GET'])
def job_status(job_id):
    job_path = os.path.join(app.config['JOBS_FOLDER'], f"{job_id}.json")
    if os.path.exists(job_path):
        with open(job_path, "r") as f:
            result = json.load(f)
        return jsonify({"job_id": job_id, "status": "completed", "result": result})
    return jsonify({"job_id": job_id, "status": "processing"})

# CLI Integration
def main():
    parser = argparse.ArgumentParser(description="ComparisonTrade AI Tool")
    parser.add_argument("--generate-rules", nargs=2, metavar=('BASELINE', 'CANDIDATE'), help="Generate rules_config.json dynamically")
    parser.add_argument("--compare", nargs=3, metavar=('BASELINE', 'CANDIDATE', 'FILE_TYPE'), help="Compare datasets using AI-generated rules with Job ID tracking")
    args = parser.parse_args()
    
    if args.generate_rules:
        generate_ai_rules(args.generate_rules[0], args.generate_rules[1])
    elif args.compare:
        job_data = {"df_baseline": args.compare[0], "df_candidate": args.compare[1], "file_type": args.compare[2]}
        response = compare_files(job_data)
        print("Comparison Job Submitted:", response.get_json())
    else:
        parser.print_help()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        main()
    else:
        app.run(host='0.0.0.0', port=5000, debug=True)
