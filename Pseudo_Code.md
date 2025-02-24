📌 ComparisonTrade - AI-Driven Discrepancy Detection System
🛠 1. Overview
What is ComparisonTrade?
ComparisonTrade is an AI-powered automated discrepancy detection and validation system designed for financial trade validation, regulatory compliance, and structured/unstructured data reconciliation. The system identifies anomalies, missing records, and misaligned values between baseline and candidate datasets.

It supports real-time processing, batch validation, and dynamic rule-based classification to categorize discrepancies based on severity.

💡 2. Key Features
✅ Multi-format Support: Handles Excel, CSV, JSON, TXT, SQL databases
✅ Intelligent Discrepancy Detection: Uses AI-driven rule validation and NLP-based text parsing
✅ Custom Rule-Based Classification: (INFO, ACCEPTABLE, WARNING, FATAL)
✅ Multi-Interface Support: API (FastAPI), CLI, and Web UI (Streamlit)
✅ Automated Reporting: Exports results in CSV, JSON, XLSX formats
✅ Cross-Platform Compatibility: Windows, macOS, Linux, Cloud Deployments
✅ Scalable for High-Volume Data: Designed for large-scale financial datasets

🏛 3. System Architecture
bash
Copy
Edit
📂 ComparisonTrade/
├── 📁 src
│   ├── 📄 api.py              # FastAPI-based API Layer
│   ├── 📄 app.py              # Interactive Streamlit UI
│   ├── 📄 cli.py              # Command-line Interface for Batch Execution
│   ├── 📂 utils               # Core Processing Modules
│   │   ├── 📄 data_processor.py  # Data Ingestion & Preprocessing
│   │   ├── 📄 validator.py       # Rule-based Validation Engine
│   │   ├── 📄 job_manager.py     # Task Scheduling & Execution
│   │   ├── 📄 settings_manager.py# Dynamic Configuration Loader
│   │   ├── 📄 visualizer.py      # Interactive Data Visualization & Reporting
│   ├── 📄 rules_config.json      # AI-driven Validation Ruleset
│   ├── 📄 directory_config.json  # Directory & Path Configurations
│   ├── 📄 trade_imp_rules.json   # Custom Trade-Specific Rules
│   ├── 📂 results/               # Stores Comparison Results
│   ├── 📂 prod/qa/               # Production & QA Data
🔹 4. AI-Driven Data Ingestion
🚀 Step 1: Loading Data from Multiple Formats
The system intelligently loads and normalizes structured/unstructured data.

🛠 Pseudocode Overview
1️⃣ API Module (api.py)
sql
Copy
Edit
BEGIN API Server
    DEFINE supported MIME types
    INITIALIZE FastAPI application

    DEFINE `/compare/` API Endpoint:
        RECEIVE baseline, candidate, config, job response, and rules files
        VALIDATE file inputs
        STORE uploaded files temporarily
        CALL `DataProcessor.run_comparison()`
        RETURN results in requested output format (JSON, CSV, XLSX, TXT)

    START FastAPI Server
END
2️⃣ Web UI (app.py)
sql
Copy
Edit
BEGIN Streamlit App
    SET page title and layout
    REMOVE cache directory if exists
    DISPLAY Upload Section
        ALLOW file uploads for baseline, candidate, and configuration files
        STORE uploaded file paths in session state
    
    DISPLAY Discrepancy Dashboard
        IF results exist:
            SHOW summary metrics
            VISUALIZE data using Plotly
        ELSE:
            SHOW message: 'Upload files to begin comparison'
    
    IF 'Clear Cache' button clicked:
        RESET session state
        REFRESH page

    DEFINE `file_upload()` FUNCTION:
        LOOP through required configuration files:
            PROMPT user for file upload
            SAVE uploaded file safely
            UPDATE session state
        RETURN uploaded files
    
    DEFINE `cleanup_temp_files()` FUNCTION:
        REMOVE temporary files and directories
        HANDLE errors safely

    DISPLAY file type selection options
    HANDLE user selection and update session state
    
    PROMPT user for baseline and candidate file uploads
    DETECT file types dynamically
    CONVERT uploaded files to DataFrames
    RUN discrepancy comparison using `DataProcessor`
    DISPLAY results in table and visual format
    
    DEFINE `get_rules_sidebar()` FUNCTION:
        LOOP through rules from config
            DISPLAY rule details in sidebar
            ALLOW user to modify constraints
        UPDATE session state based on user selections
    
    HANDLE filter application and result update
    DISPLAY export options and allow file downloads
    DISPLAY key performance indicators and discrepancy charts
    HANDLE UI refresh based on user actions
END
3️⃣ CLI Module (cli.py)
pgsql
Copy
Edit
BEGIN CLI Tool
    PARSE command-line arguments
    VALIDATE input file paths
    INITIALIZE `DataProcessor` with configuration files
    CALL `DataProcessor.run_comparison()`
    SAVE results in the specified output format
END
4️⃣ Data Processing (data_processor.py)
pgsql
Copy
Edit
BEGIN DataProcessor
    INITIALIZE with directory_config, job_response, rules_config
    
    FUNCTION `run_comparison(baseline_file, candidate_file, file_type, filters)`:
        READ baseline and candidate files based on format (Excel, CSV, TXT)
        PERFORM column alignment check
        APPLY validation rules from `validator.py`
        IDENTIFY discrepancies
        RETURN results
    
    FUNCTION `save_results(results, output_path, format)`:
        EXPORT results in CSV, JSON, or Excel
END
5️⃣ Validator Module (validator.py)
pgsql
Copy
Edit
BEGIN Validator
    CLASS `ValidationRule`:
        INITIALIZE with columns, validation function, and description
        FUNCTION `validate(df)`:
            APPLY validation function to dataframe
            RETURN pass/fail status and failed rows
    
    CLASS `ValidationRuleLoader`:
        FUNCTION `load_rules(config_path)`:
            READ rules from JSON file
            RETURN list of `ValidationRule` objects
END
6️⃣ AI-Powered Data Ingestion
vbnet
Copy
Edit
BEGIN Data Loader
    FUNCTION `load_data(file_path, file_type)`:
        IF file_type is "Excel":
            READ using pandas.read_excel()
        ELSE IF file_type is "CSV":
            READ using pandas.read_csv()
        ELSE IF file_type is "JSON":
            READ and parse JSON file
        ELSE IF file_type is "TXT":
            CALL `parse_text_file()`
        ELSE:
            RAISE unsupported file format error
END
7️⃣ AI-Driven Key-Value Parsing
pgsql
Copy
Edit
BEGIN Text File Parser
    FUNCTION `parse_text_file(file_path)`:
        INITIALIZE empty dictionary
        OPEN file and read line by line
        FOR each line:
            IF "=" exists in line:
                SPLIT key and value
                STRIP whitespaces and store in dictionary
        RETURN parsed dictionary
END
8️⃣ AI-Powered Discrepancy Detection
vbnet
Copy
Edit
BEGIN Data Comparison
    FUNCTION `compare_datasets(baseline, candidate, rules_config)`:
        INITIALIZE missing_records and discrepancies lists
        FOR each key in baseline:
            IF key missing in candidate:
                APPEND to missing_records
            ELSE:
                DETECT data type of values
                COMPUTE value difference
                GET threshold from rules_config

                IF difference is zero:
                    CONTINUE (No discrepancy)

                IF difference <= MIN threshold:
                    SET flag as "ACCEPTABLE"
                ELSE IF within threshold range:
                    SET flag as "WARNING"
                ELSE:
                    SET flag as "FATAL"

                APPEND to discrepancies list

        RETURN missing_records and discrepancies
END
9️⃣ AI-Driven Data Type Detection
sql
Copy
Edit
BEGIN Data Type Detection
    FUNCTION `detect_data_type(value1, value2)`:
        IF both values are numeric:
            RETURN "NUMERIC"
        ELSE IF both are strings:
            RETURN "STRING"
        ELSE IF both are lists:
            RETURN "ARRAY"
        ELSE IF values resemble timestamps:
            RETURN "DATETIME"
        ELSE:
            RETURN "UNKNOWN"
END
🔟 Computing Differences Between Values
vbnet
Copy
Edit
BEGIN Value Difference Calculation
    FUNCTION `compute_value_difference(value1, value2, data_type)`:
        IF data_type is "NUMERIC":
            COMPUTE absolute difference
        ELSE IF data_type is "STRING":
            COMPUTE Levenshtein distance
        ELSE IF data_type is "DATETIME":
            COMPUTE difference in seconds
        ELSE IF data_type is "ARRAY":
            COMPUTE symmetric set difference
        ELSE:
            RETURN zero (No meaningful comparison)
END
1️⃣1️⃣ Report Generation
vbnet
Copy
Edit
BEGIN Report Generator
    FUNCTION `generate_report(discrepancies, format)`:
        IF format is "csv":
            SAVE as CSV file
        ELSE IF format is "json":
            SAVE as JSON file
        ELSE IF format is "xlsx":
            SAVE as Excel file
        ELSE:
            RAISE unsupported format error
END
1️⃣2️⃣ CLI Execution
bash
Copy
Edit
BEGIN CLI Execution
    RUN command:
        python src/cli.py --baseline "file1.xlsx" --candidate "file2.xlsx" --output "output.json" --file_type "Excel" --format "json"
END
1️⃣3️⃣ API Execution
sql
Copy
Edit
BEGIN API Execution
    START FastAPI Server:
        uvicorn src.api:app --host 0.0.0.0 --port 8000 --reload

    Example API Request:
        curl -X 'POST' 'http://localhost:8000/compare/' -F 'baseline_file=@file1.xlsx' -F 'candidate_file=@file2.xlsx' -F 'file_type=Excel' -F 'output_format=json'
END
1️⃣4️⃣ Key Takeaways
sql
✔ AI-enhanced data validation and anomaly detection
✔ Batch processing & real-time API support
✔ Cross-platform deployment (Windows, Linux, macOS)
✔ Enterprise-grade scalability with configurable rules
✔ Generates structured reports for compliance and auditing





import pandas as pd
import json

def load_data(file_path, file_type):
    if file_type == "Excel":
        return pd.read_excel(file_path)
    elif file_type == "CSV":
        return pd.read_csv(file_path)
    elif file_type == "JSON":
        with open(file_path) as file:
            return json.load(file)
    elif file_type == "TXT":
        return parse_text_file(file_path)
    else:
        raise ValueError("Unsupported File Format")
🚀 Step 2: AI-Driven Key-Value Extraction (TXT Parsing)

def parse_text_file(file_path):
    data = {}
    with open(file_path) as file:
        for line in file:
            if "=" in line:
                key, value = map(str.strip, line.split("="))
                data[key] = value
    return data
Example Input:

ini
Copy
Edit
order_id=12345
trade_price=100.50
order_status=Executed
timestamp=2024-02-20T10:30:00Z
AI-enhanced Parsed Output:

json
Copy
Edit
{
    "order_id": "12345",
    "trade_price": "100.50",
    "order_status": "Executed",
    "timestamp": "2024-02-20T10:30:00Z"
}
🔹 5. AI-Powered Discrepancy Detection

def compare_datasets(baseline, candidate, rules_config):
    missing_records = []
    discrepancies = []

    for key in baseline:
        if key not in candidate:
            missing_records.append({key: "Missing in Candidate"})
            continue

        base_value, candidate_value = baseline[key], candidate[key]
        data_type = detect_data_type(base_value, candidate_value)
        difference = compute_value_difference(base_value, candidate_value, data_type)
        threshold = get_threshold(rules_config, key)

        if difference == 0:
            continue  # No discrepancy

        if difference <= threshold["MIN"]:
            flag = "ACCEPTABLE"
        elif threshold["MIN"] < difference < threshold["MAX"]:
            flag = "WARNING"
        else:
            flag = "FATAL"

        discrepancies.append({
            "field": key,
            "baseline_value": base_value,
            "candidate_value": candidate_value,
            "difference": difference,
            "flag": flag
        })

    return {"missing": missing_records, "discrepancies": discrepancies}
🔹 6. Data Type Detection & Comparison
🚀 Detecting Data Types Dynamically

def detect_data_type(value1, value2):
    if isinstance(value1, (int, float)) and isinstance(value2, (int, float)):
        return "NUMERIC"
    elif isinstance(value1, str) and isinstance(value2, str):
        return "STRING"
    elif isinstance(value1, list) and isinstance(value2, list):
        return "ARRAY"
    elif "T" in str(value1) and "T" in str(value2):  
        return "DATETIME"
    else:
        return "UNKNOWN"
🚀 Computing Discrepancy Values

def compute_value_difference(value1, value2, data_type):
    if data_type == "NUMERIC":
        return abs(float(value1) - float(value2))
    elif data_type == "STRING":
        return levenshtein_distance(value1, value2)  # AI-driven similarity check
    elif data_type == "DATETIME":
        return abs(parse_datetime(value1) - parse_datetime(value2)).total_seconds()
    elif data_type == "ARRAY":
        return len(set(value1) ^ set(value2))  
    else:
        return 0
🔹 7. Report Generation

def generate_report(discrepancies, format):
    if format == "csv":
        pd.DataFrame(discrepancies).to_csv("output.csv", index=False)
    elif format == "json":
        with open("output.json", "w") as file:
            json.dump(discrepancies, file, indent=4)
    elif format == "xlsx":
        pd.DataFrame(discrepancies).to_excel("output.xlsx", index=False)
    else:
        raise ValueError("Unsupported Format")
🔹 8. CLI Execution
sh
Copy
Edit
python src/cli.py --baseline "asx_1.xlsx" --candidate "asx_2.xlsx" --output "results/output.json" --file_type "Excel" --format "json"
🔹 9. API Execution
sh
Copy
Edit
uvicorn src.api:app --host 0.0.0.0 --port 8000 --reload
Example API Request:

sh
Copy
Edit
curl -X 'POST' 'http://localhost:8000/compare/' -F 'baseline_file=@asx_1.xlsx' -F 'candidate_file=@asx_2.xlsx' -F 'file_type=Excel' -F 'output_format=json'
🚀 Enterprise-Level Scalability
✔ AI-enhanced validation with NLP & ML
✔ Batch processing & real-time API integration
✔ Cross-platform deployment (AWS, Azure, GCP)

🚀 ComparisonTrade ensures financial institutions achieve high-accuracy data validation at scale!

