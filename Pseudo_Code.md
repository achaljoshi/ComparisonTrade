# 📌 ComparisonTrade - AI-Driven Discrepancy Detection System
# 🛠 1. Overview
What is ComparisonTrade?

ComparisonTrade is an AI-powered automated discrepancy detection and validation system designed for financial trade validation, regulatory compliance, and structured/unstructured data reconciliation. The system identifies anomalies, missing records, and misaligned values between baseline and candidate datasets.

It supports real-time processing, batch validation, and dynamic rule-based classification to categorize discrepancies based on severity.

# 💡 2. Key Features
✅ Multi-format Support: Handles Excel, CSV, JSON, TXT, SQL databases
✅ Intelligent Discrepancy Detection: Uses AI-driven rule validation and NLP-based text parsing
✅ Custom Rule-Based Classification: (INFO, ACCEPTABLE, WARNING, FATAL)
✅ Multi-Interface Support: API (FastAPI), CLI, and Web UI (Streamlit)
✅ Automated Reporting: Exports results in CSV, JSON, XLSX formats
✅ Cross-Platform Compatibility: Windows, macOS, Linux, Cloud Deployments
✅ Scalable for High-Volume Data: Designed for large-scale financial datasets

# 📂 3.  Directory Structure
```
├── src
│   ├── api.py                 # API Layer (FastAPI)
│   ├── app.py                 # Streamlit UI
│   ├── cli.py                 # CLI Tool
│   ├── utils                  # Utility Modules
│   │   ├── data_processor.py  # Data Parsing & Processing
│   │   ├── validator.py       # Validation & Discrepancy Detection
│   │   ├── job_manager.py     # Job Handling
│   │   ├── settings_manager.py# Configuration Management
│   │   ├── visualizer.py      # Reporting & Visualization
│   ├── rules_config.json      # Rule Configuration
│   ├── directory_config.json  # Directory Paths
│   ├── order_book_rules.json  # Sample Rules for Orders
│   ├── trade_imp_rules.json   # Sample Rules for Trades
│   ├── results/               # Stores comparison results
│   ├── prod/qa/               # Production & QA Data
```

---
# 4. Data Ingestion

### 🚀 Step 1: Loading Data from Multiple Formats**
The system intelligently loads and normalizes structured/unstructured data.

### 🛠 Pseudocode Overview
#### 1️⃣ API Module (api.py)
```
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
```
#### 2️⃣ Web UI (app.py)
```
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
```
#### 3️⃣ CLI Module (cli.py)
```
BEGIN CLI Tool
    PARSE command-line arguments
    VALIDATE input file paths
    INITIALIZE `DataProcessor` with configuration files
    CALL `DataProcessor.run_comparison()`
    SAVE results in the specified output format
END
```
#### 4️⃣ Data Processing (data_processor.py)
```
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
```
#### 5️⃣ Validator Module (validator.py)
```
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
```
#### 6️⃣ Data Ingestion
```
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
```
#### 7️⃣ Key-Value Parsing
```
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
```
#### 8️⃣ Discrepancy Detection
```
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
```
#### 9️⃣ Computing Differences Between Values
```
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
```
#### 🔟 Report Generation
```
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
```
#### 1️⃣1️⃣ CLI Execution
```
BEGIN CLI Execution
    RUN command:
        python src/cli.py --baseline "file1.xlsx" --candidate "file2.xlsx" --output "output.json" --file_type "Excel" --format "json"
END
```
#### 1️⃣2️⃣ API Execution
```
BEGIN API Execution
    START FastAPI Server:
        uvicorn src.api:app --host 0.0.0.0 --port 8000 --reload

    Example API Request:
        curl -X 'POST' 'http://localhost:8000/compare/' -F 'baseline_file=@file1.xlsx' -F 'candidate_file=@file2.xlsx' -F 'file_type=Excel' -F 'output_format=json'
END 
```
## Key Takeaways

✔ Dynamic data validation and anomaly detection

✔ Batch processing & real-time API support

✔ Cross-platform deployment (Windows, Linux, macOS)

✔ Enterprise-grade scalability with configurable rules

✔ Generates structured reports for compliance and auditing




## Step 1: Load Data using data parser methods
```python
def read_file(self, file, file_type: str, chunk_size=500) -> pd.DataFrame:

    # ✅ Handle UploadedFile or BytesIO (for Streamlit uploaded files)
    if isinstance(file, BytesIO) or hasattr(file, "read"):
        file.seek(0)  # ✅ Reset pointer for reading
        file_bytes = file.read()
        if file_type == "Excel":
            return self.read_excel_file(BytesIO(file_bytes), chunk_size)

        elif file_type == "DD":
            return pd.concat(
                self.read_dd_file(BytesIO(file_bytes), chunk_size),
                ignore_index=True,
            )

        elif file_type in {"TEXT", "CSV", "JSON", "LOG"}:
            return self.read_logs_file(file, file_type, chunk_size)
        else:
            raise ValueError(f"Unsupported file type: {file_type}")
```
## Step 2: Parse Key-Value Pairs from DD Files
```python
def read_dd_file(self, file: Union[str, BytesIO], chunk_size) -> Generator[pd.DataFrame, None, None]:
    .....
    .....
    .....
if delimiter in line: # type: ignore
            key, value = map(str.strip, line.split("=", 1))

            if line.__contains__("[") and line.__contains__("]"):
                if array_match := re.match(r"(\w+)\s*=\s*(\[.*\])", line):
                    arr_key, arr_values = array_match.groups()

                    if arr_key in array_fields:
                        array_data = {
                            int(k): int(v)
                            for k, v in re.findall(r"\[(\d+)=(\d+)\]", arr_values)
                        }
                        current_record[arr_key] = array_data
                    continue

            # Handle nested object start
            if value.startswith("{") and not value.endswith("}"):
                current_key = key
                stack.append((current_record, current_key))
                current_record = {}
                continue

            # Inline nested object parsing
            if value.startswith("{") and value.endswith("}"):
                current_record[key] = self.parse_nested_object(value)
                continue

            # Array parsing
            if "[" in value and "]" in value:
                current_record[key] = self.parse_array(value)
                continue

            # Default key-value assignment
            current_record[key] = value

    # ✅ Process remaining records
    if records:
        yield pd.DataFrame(records)
```
**Example Input:**
```
{
    messageGroup = 10
    messageId = 1
    tradeTime = 1737600843524975960
    orderBookId = 71162
    userId = 4915200
    participantId = 75
    orderId = 7960211576596756141
    quoteMessageId = 0
}
```
**Parsed Output:**
```json
{
    "messageGroup" : 10,
    "messageId" : 1,
    "tradeTime" : 1737600843524975960,
    "orderBookId" : 71162,
    "userId" : 4915200,
    "participantId" : 75,
    "orderId" : 7960211576596756141,
    "quoteMessageId" : 0
}
```
## Step 3: Compare Baseline vs. Candidate Data
```python
def compare_files(self, df_baseline=None, df_candidate=None, file_type="Excel", filters=None):
    if filters is None:
            filters = {}

    if df_baseline is not None and df_candidate is not None:
        df_baseline, df_candidate = df_baseline.copy(), df_candidate.copy()
    else:
        input_file_baseline, input_file_candidate, _ = self.resolve_file_paths()
    ....
    df_baseline.columns = df_baseline.columns.astype(str).str.strip()
    df_candidate.columns = df_candidate.columns.astype(str).str.strip()
    df_merged = df_baseline.merge(df_candidate,on=key_column,suffixes=("_baseline", "_candidate"),how="outer",indicator=True,)

    df_merged = df_merged[df_merged["_merge"] == "both"]
    ....
    ....
    # ✅ Identify Missing Rows Before Applying Rules
    extra_rows_candidate = df_candidate[~df_candidate[key_column].isin(df_baseline[key_column])]
    extra_rows_baseline = df_baseline[~df_baseline[key_column].isin(df_candidate[key_column])]
    discrepancies = []
    if array_rule_key:
        self.get_arrays_rules(df_merged, discrepancies, key_column, array_rule_key)

    # ✅ For handling normal objects
    self.extract_discrepancy(df_merged, discrepancies, filters, key_column)

    # ✅ Include Missing Rows
    self.missing_row_candidate(discrepancies, extra_rows_baseline, key_column)

    self.missing_row_baseline(discrepancies, extra_rows_candidate, key_column)
    discrepancies_df = pd.DataFrame(discrepancies)
    discrepancies_df = discrepancies_df.astype(str)
    print(discrepancies_df)

    return discrepancies_df
```
## Step 4: Compute Differences based on datatype
```python
def extract_discrepancy(self, df_merged, discrepancies, filters, key_column):
    updated_constraint_min = filters.get(rule_number, {}).get("constraints_min", rule.get("constraints", {}).get("min", float("inf")))
    updated_constraint_max = filters.get(rule_number, {}).get("constraints_max", rule.get("constraints", {}).get("max", float("inf")))

    if "constraints" in rule:
        df_merged["rule_violation"] = abs(
            pd.to_numeric(df_merged[col_candidate], errors="coerce")
            - pd.to_numeric(
                df_merged[col_baseline], errors="coerce"
            )
        )
        self._extracted_discrepancy_classification(
            rule,
            df_merged,
            updated_constraint_min,
            updated_constraint_max,
        )
    else:
        df_merged["rule_violation"] = df_merged.apply(
            lambda row: row[col_baseline] != row[col_candidate],
            axis=1,
        )
```
##  Step 5: Classify Differences based on Rules Config file
```python
def _extracted_discrepancy_classification(self, rule, df_merged, arg2, arg3, classification):
        df_merged.loc[df_merged["rule_violation"] <= arg2, "classification"] = rule.get(
            "category", classification
        )
        df_merged.loc[df_merged["rule_violation"] >= arg3, "classification"] = rule.get(
            "category", classification
        )
        df_merged.loc[
            (df_merged["rule_violation"] > arg2) & (df_merged["rule_violation"] < arg3),
            "classification",
        ] = classification
```
##  Step 6: Generate & Export Report
```python
def discrepancy_list(self,df_merged,discrepancies,key_column,rule_number,rule_type, rule_description,output_column_name,base_val,cand_val,):
    for _, row in df_merged[df_merged["rule_violation"] > 0].iterrows():
        discrepancies.append(
            {
                key_column: row[key_column],
                "Column Name": output_column_name,
                "Rule Type": rule_type,
                "category": row["classification"],
                "Rule Number": rule_number,
                "Description": rule_description,
                "Baseline Field Value": base_val,
                "Candidate Field Value": cand_val,
            }
        )

def save_results(self, results, output_path, format="csv"):
        """Save results in the required format."""
        if format == "csv":
            results.to_csv(output_path, index=False)
        elif format == "json":
            results.to_json(output_path, orient="records", indent=4)
        elif format == "excel":
            results.to_excel(output_path, index=False)
        print(f"Results saved at {output_path}")
```
##  Step 7: CLI Execution
```
python src/cli.py --baseline "asx_1.txt" --candidate "asx_2.txt" --output "results/output.json" --file_type "txt" --format "json"
```
##  Step 8: API Execution (FastAPI)
```
** Start hosting API **
uvicorn src.api:app --host 0.0.0.0 --port 8000 --reload
Example API Request:

curl -X 'POST' 'http://localhost:8000/compare/' -F 'baseline_file=@asx_1.txt' -F 'candidate_file=@asx_2.txt' -F 'file_type=txt' -F 'output_format=json'
```
🚀 Enterprise-Level Scalability
✔ AI-enhanced validation with NLP & ML
✔ Batch processing & real-time API integration
✔ Cross-platform deployment (AWS, Azure, GCP)

🚀 ComparisonTrade ensures financial institutions achieve high-accuracy data validation at scale!

