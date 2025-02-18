# Discrepancy Analysis Tool

A robust tool for analyzing discrepancies between baseline and candidate files, supporting multiple file formats and configurable comparison rules.

## Features

- **Multiple File Format Support**
  - Excel (.xlsx)
  - DD Files (.log, .csv)
  - Text Files (.txt)

- **Flexible Comparison Rules**
  - Threshold-based comparisons
  - Date comparisons
  - String matching
  - Missing record detection
  - Customizable filters

- **Multiple Interfaces**
  - Web Dashboard (Streamlit)
  - REST API (FastAPI)
  - Command Line Interface (CLI)

## Installation


git clone https://github.com/yourusername/discrepancy-analysis-tool.git
cd discrepancy-analysis-tool
pip install -r requirements.txt


## Usage
Web Dashboard
streamlit run src/app.py

API Server
uvicorn src.api:app --reload

CLI
python src/cli.py compare --baseline path/to/baseline.xlsx --candidate path/to/candidate.xlsx --directory-config config/directory_config.json --job-response config/job_response.json --rules-config config/rules_config.json --file-type Excel --output results.csv


## Configuration
directory_config.json
{
    "input_base_dir_baseline": "path/to/baseline/files",
    "input_base_dir_candidate": "path/to/candidate/files",
    "output_base_dir": "path/to/output"
}

job_response.json
{
    "baseline": {
        "env": "PROD",
        "label": "20240101"
    },
    "candidate": {
        "env": "QA",
        "label": "20240101"
    },
    "report": {
        "format": "CSV"
    }
}
rules_config.json
{
    "identifier": "ID",
    "rules": [
        {
            "Rule Number": "RULE_001",
            "type": "threshold",
            "columns": ["Amount"],
            "acceptable": 0.01,
            "warning": {
                "min": 0.01,
                "max": 0.1
            },
            "fatal": {
                "min": 0.1
            }
        }
    ]
}
Project Structure
discrepancy-analysis-tool/
├── src/
│   ├── app.py
│   ├── api.py
│   ├── cli.py
│   └── utils/
│       └── data_processor.py
├── config/
│   ├── directory_config.json
│   ├── job_response.json
│   └── rules_config.json
├── tests/
├── requirements.txt
└── README.md

Key Components
DataProcessor: Core comparison engine
Web Dashboard: Interactive UI for file comparison
REST API: HTTP endpoints for integration
CLI: Command-line interface for automation
Performance Features
Chunked processing for large datasets
Parallel computation support
Memory-efficient operations
Configurable batch sizes
Background task processing
Output Formats
CSV
Excel
JSON
Text
Requirements
See requirements.txt for complete list of dependencies.

License
MIT License

Contributing
Fork the repository
Create your feature branch
Commit your changes
Push to the branch
Create a Pull Request