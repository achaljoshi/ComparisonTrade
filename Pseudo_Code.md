# 📌 ComparisonTrade - Automated Discrepancy Detection System

## 🛠 System Overview
ComparisonTrade is a powerful tool designed for automated **discrepancy detection and validation** across multiple data formats, ensuring accuracy in financial trade logs and other structured/unstructured data files.

### **Key Features**
- ✅ Supports structured/unstructured data (Excel, CSV, JSON, TXT)
- ✅ Automated key-value pair discrepancy detection
- ✅ Dynamic rule-based classification (INFO, ACCEPTABLE, WARNING, FATAL)
- ✅ Cross-platform compatibility (Windows, macOS, Linux)
- ✅ Operates via CLI & API
- ✅ Generates detailed reports (CSV, JSON, XLSX)
- ✅ Scalable for cloud deployment

---

## 📂 Directory Structure
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

## 🔹 Step 1: Load & Parse Data
Supports multiple formats: **Excel, CSV, JSON, TXT**
```python
def load_data(file_path, file_type):
    if file_type == "Excel":
        return pandas.read_excel(file_path)
    elif file_type == "CSV":
        return pandas.read_csv(file_path)
    elif file_type == "JSON":
        with open(file_path) as file:
            return json.load(file)
    elif file_type == "TXT":
        return parse_text_file(file_path)
    else:
        raise ValueError("Unsupported File Format")
```

---

## 🔹 Step 2: Parse Key-Value Pairs from TXT Files
```python
def parse_text_file(file_path):
    data = {}
    with open(file_path) as file:
        for line in file:
            if "=" in line:
                key, value = map(str.strip, line.split("="))
                data[key] = value
    return data
```
**Example Input (TXT File)**
```
order_id=12345
trade_price=100.50
order_status=Executed
timestamp=2024-02-20T10:30:00Z
```
**Parsed Output**
```json
{
    "order_id": "12345",
    "trade_price": "100.50",
    "order_status": "Executed",
    "timestamp": "2024-02-20T10:30:00Z"
}
```

---

## 🔹 Step 3: Compare Baseline vs. Candidate Data
```python
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
```

---

## 🔹 Step 4: Handling Different Data Types
```python
def detect_data_type(value1, value2):
    if isinstance(value1, (int, float)) and isinstance(value2, (int, float)):
        return "NUMERIC"
    elif isinstance(value1, str) and isinstance(value2, str):
        return "STRING"
    elif isinstance(value1, list) and isinstance(value2, list):
        return "ARRAY"
    elif "T" in str(value1) and "T" in str(value2):  # Simple check for timestamps
        return "DATETIME"
    else:
        return "UNKNOWN"
```

---

## 🔹 Step 5: Compute Value Differences
```python
def compute_value_difference(value1, value2, data_type):
    if data_type == "NUMERIC":
        return abs(float(value1) - float(value2))
    elif data_type == "STRING":
        return levenshtein_distance(value1, value2)
    elif data_type == "DATETIME":
        return abs(parse_datetime(value1) - parse_datetime(value2)).total_seconds()
    elif data_type == "ARRAY":
        return len(set(value1) ^ set(value2))  # Symmetric difference in sets
    else:
        return 0
```

---

## 🔹 Step 6: Generate & Export Report
```python
def generate_report(discrepancies, format):
    if format == "csv":
        pandas.DataFrame(discrepancies).to_csv("output.csv", index=False)
    elif format == "json":
        with open("output.json", "w") as file:
            json.dump(discrepancies, file, indent=4)
    elif format == "xlsx":
        pandas.DataFrame(discrepancies).to_excel("output.xlsx", index=False)
    else:
        raise ValueError("Unsupported Format")
```

---

## 🔹 Step 7: CLI Execution
To run the system using CLI:
```sh
python src/cli.py --baseline "asx_1.xlsx" --candidate "asx_2.xlsx" --output "results/output.json" --file_type "Excel" --format "json"
```

---

## 🔹 Step 8: API Execution (FastAPI)
Start the API Server:
```sh
uvicorn src.api:app --host 0.0.0.0 --port 8000 --reload
```

Example API Request:
```sh
curl -X 'POST' 'http://localhost:8000/compare/' -F 'baseline_file=@asx_1.xlsx' -F 'candidate_file=@asx_2.xlsx' -F 'file_type=Excel' -F 'output_format=json'
```

---

# ✅ Key Takeaways
✔ Parses and compares key-value pairs dynamically  
✔ Handles multiple file formats (Excel, CSV, JSON, TXT)  
✔ Supports multiple data types (Numeric, String, Datetime, Array)  
✔ Categorizes discrepancies into **INFO, ACCEPTABLE, WARNING, FATAL**  
✔ Generates detailed reports in **CSV, JSON, XLSX**  
✔ Fully **cross-platform** (Windows, macOS, Linux)  

🚀 **ComparisonTrade ensures accurate, efficient, and scalable data validation.**