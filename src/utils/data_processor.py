import json  # ✅ Fix: Ensure JSON module is imported
import os
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path


class DataProcessor:
    def __init__(self, directory_config, job_response, rules_config):
        """Initialize with file paths or direct dictionary data."""
        
        # ✅ Handle if config is a file path or a dictionary
        if isinstance(directory_config, str) and os.path.exists(directory_config):
            with open(directory_config, "r") as file:
                self.directory_config = json.load(file)
        else:
            self.directory_config = directory_config  # Assume it's already a dictionary

        if isinstance(job_response, str) and os.path.exists(job_response):
            with open(job_response, "r") as file:
                self.job_response = json.load(file)
        else:
            self.job_response = job_response  # Assume it's already a dictionary

        if isinstance(rules_config, str) and os.path.exists(rules_config):
            with open(rules_config, "r") as file:
                self.rules_config = json.load(file)
        else:
            self.rules_config = rules_config  # Assume it's already a dictionary
    
    def run_comparison(self, baseline_file, candidate_file, file_type="Excel", filters=None):
        """Run the discrepancy check process."""
        if isinstance(baseline_file, BytesIO):
            df_baseline = pd.read_excel(baseline_file, engine="openpyxl") if file_type == "Excel" else pd.read_csv(baseline_file)
        else:
            df_baseline = pd.read_excel(baseline_file, engine="openpyxl") if file_type == "Excel" else pd.read_csv(baseline_file)

        if isinstance(candidate_file, BytesIO):
            df_candidate = pd.read_excel(candidate_file, engine="openpyxl") if file_type == "Excel" else pd.read_csv(candidate_file)
        else:
            df_candidate = pd.read_excel(candidate_file, engine="openpyxl") if file_type == "Excel" else pd.read_csv(candidate_file)

        results = self.compare_files(df_baseline, df_candidate, file_type, filters)
        return results

    def save_results(self, results, output_path, format="csv"):
        """Save results in the required format."""
        if format == "csv":
            results.to_csv(output_path, index=False)
        elif format == "json":
            results.to_json(output_path, orient="records", indent=4)
        elif format == "excel":
            results.to_excel(output_path, index=False)
        print(f"Results saved at {output_path}")

    def resolve_file_paths(self):
        """Resolve actual file paths using job response and directory config."""
        baseline_env = self.job_response["baseline"]["env"]
        baseline_label = self.job_response["baseline"]["label"]
        candidate_env = self.job_response["candidate"]["env"]
        candidate_label = self.job_response["candidate"]["label"]

        # ✅ Ensure paths are properly formatted
        input_file_baseline = Path(self.directory_config["input_file_baseline"].format(
            input_base_dir_baseline=self.directory_config["input_base_dir_baseline"],
            ENV=baseline_env,
            DD_file_date=baseline_label
        )).resolve()

        input_file_candidate = Path(self.directory_config["input_file_candidate"].format(
            input_base_dir_candidate=self.directory_config["input_base_dir_candidate"],
            ENV=candidate_env,
            DD_file_date=candidate_label
        )).resolve()

        output_file_result = Path(self.directory_config["output_file_result"].format(
            output_base_dir=self.directory_config["output_base_dir"],
            BASELINE_ENV=baseline_env,
            CANDIDATE_ENV=candidate_env,
            DD_file_date=baseline_label,
            rundate=candidate_label
        )).resolve()

        return input_file_baseline, input_file_candidate, output_file_result

    def read_text_file(self, file_path: Path) -> pd.DataFrame:
       
     records = []
     current_record = {}
     inside_record = False  # ✅ Track whether we're inside a `{}` block

     with open(file_path, "r") as file:
         for line in file:
             line = line.strip()

             # ✅ Start a new record
             if line == "{":
                 inside_record = True
                 current_record = {}  # Start new record
                 continue

             # ✅ End of record
             elif line == "}":
                 inside_record = False
                 if current_record:  # ✅ Store only non-empty records
                     records.append(current_record)
                 current_record = {}
                 continue

             # ✅ Extract key-value pairs correctly (only inside `{}` block)
             if inside_record and "=" in line:
                 key, value = map(str.strip, line.split("=", 1))
                 current_record[key] = value

     # ✅ Append last record if it exists
     if current_record:
         records.append(current_record)

     # ✅ Convert parsed records to a DataFrame
     df = pd.DataFrame(records)

     # ✅ Debugging: Print parsed results
     print("Parsed DataFrame:\n", df.head())
     print("Columns after parsing:", df.columns.tolist())

     return df

    def read_dd_file(self, file_path: Path) -> pd.DataFrame:
        """Read a DD file (.log or .csv) with appropriate delimiters."""
        if file_path.suffix.lower() == ".log":
            return pd.read_csv(file_path, delimiter="|", header=0)
        elif file_path.suffix.lower() == ".csv":
            return pd.read_csv(file_path, delimiter=",", header=0)
        else:
            raise ValueError(f"Unsupported DD file format: {file_path.suffix}")
    
    def read_file(self, file_path: Path, file_type: str) -> pd.DataFrame:
        """Determine file type and read accordingly."""
        if file_type == "Text":
            return self.read_text_file(file_path)
        elif file_type == "DD":
            return self.read_dd_file(file_path)
        elif file_type == "Excel" and file_path.suffix.lower() in [".xlsx", ".xls"]:
            return pd.read_excel(file_path, engine="openpyxl")
        else:
            raise ValueError(f"Unsupported file type: {file_path.suffix}")
  

    def compare_files(self, df_baseline=None, df_candidate=None, file_type="Excel", filters=None):
      """Compare Baseline and Candidate files using dynamically defined rules from rules_config.json."""

      if filters is None:
        filters = {}

    
      input_file_baseline, input_file_candidate, _ = self.resolve_file_paths()
      df_baseline = self.read_text_file(input_file_baseline)
      df_candidate = self.read_text_file(input_file_candidate)

      # ✅ Debugging: Print first few rows before processing
      print("Baseline Data (Before Cleaning):\n", df_baseline.head())
      print("Candidate Data (Before Cleaning):\n", df_candidate.head())

    # ✅ Ensure that rules_config is correctly loaded
     # print("Rules Config Loaded:", json.dumps(self.rules_config, indent=4))
      
      if isinstance(self.rules_config, str):
       print("🚨 ERROR: rules_config is a string instead of a dictionary! Decoding JSON...")
       self.rules_config = json.loads(self.rules_config)
       
      if "rules" not in self.rules_config or not isinstance(self.rules_config["rules"], dict):
       raise ValueError("Error: 'rules' key not found or incorrectly formatted in rules_config.json. Please check your config file.")
      
      key_column = "messageId" if "messageId" in self.rules_config["rules"] else None
    
      if not key_column:
       raise ValueError("Error: 'messageId' key not found in rules_config.json under 'rules'. Please check your config file.")

    # ✅ Strip and remove any leading/trailing spaces
      df_baseline.columns = df_baseline.columns.astype(str).str.strip()
      df_candidate.columns = df_candidate.columns.astype(str).str.strip()

    # ✅ Debugging: Print column names after stripping
      print("Baseline Columns after stripping:", df_baseline.columns.tolist())
      print("Candidate Columns after stripping:", df_candidate.columns.tolist())

    # ✅ Ensure the key_column exists in both datasets
      if key_column not in df_baseline.columns:
        print(f"Error: Key identifier '{key_column}' is missing from the Baseline dataset. Columns found: {df_baseline.columns.tolist()}")
        raise ValueError(f"Key identifier '{key_column}' not found in Baseline dataset.")

      if key_column not in df_candidate.columns:
        print(f"Error: Key identifier '{key_column}' is missing from the Candidate dataset. Columns found: {df_candidate.columns.tolist()}")
        raise ValueError(f"Key identifier '{key_column}' not found in Candidate dataset.")

      df_merged = df_baseline.merge(
        df_candidate, on=key_column, suffixes=("_baseline", "_candidate"), how="outer", indicator=True
    )

      discrepancies = []
      extra_rows_candidate = df_candidate[~df_candidate[key_column].isin(df_baseline[key_column])]
      extra_rows_baseline = df_baseline[~df_baseline[key_column].isin(df_candidate[key_column])]

      for column_name, rules in self.rules_config["rules"].items():
       for rule in rules:  # rules is a list, so iterate properly
        if not isinstance(rule, dict):
            raise ValueError(f"Error: Rule for column '{column_name}' is not formatted correctly. Expected dict, got {type(rule)}")

        rule_number = rule.get("Rule Number", "N/A")  # ✅ Use .get() safely
        rule_type = rule.get("type", "Unknown")  # ✅ Use .get() safely
        rule_description = rule.get("description", "No description available")

        for col in rule.get("columns", []):  # ✅ Ensure columns exist
            col_baseline = f"{col}_baseline"
            col_candidate = f"{col}_candidate"

            if col_baseline in df_merged.columns and col_candidate in df_merged.columns:
                df_merged["rule_violation"] = abs(
                    pd.to_numeric(df_merged[col_candidate], errors="coerce") - 
                    pd.to_numeric(df_merged[col_baseline], errors="coerce")
                )
                df_merged.loc[df_merged["rule_violation"] > 0, "classification"] = "DISCREPANCY"

                for _, row in df_merged[df_merged["classification"] == "DISCREPANCY"].iterrows():
                    discrepancies.append({
                        key_column: row[key_column],
                        "Column Name": col,
                        "Rule Type": rule_type,
                        "Category": "DISCREPANCY",
                        "Rule Number": rule_number,
                        "Description": rule_description,
                        "Baseline Field Value": row[col_baseline],
                        "Candidate Field Value": row[col_candidate]
                    })

      discrepancies_df = pd.DataFrame(discrepancies)
      discrepancies_df = discrepancies_df.astype(str)
      print(discrepancies_df)

      return discrepancies_df
