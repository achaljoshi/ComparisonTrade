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
        """Read a text file based on delimiter and header settings."""
        delimiter = self.rules_config.get("text_file_delimiter", ",")
        header = 0 if self.rules_config.get("text_file_contains_header", "yes").lower() == "yes" else None
        
        return pd.read_csv(file_path, delimiter=delimiter, header=header)
    
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

        # ✅ Load DataFrames from Uploaded Files
        if df_baseline is not None and df_candidate is not None:
            df_prod, df_qa = df_baseline.copy(), df_candidate.copy()
        else:
            input_file_baseline, input_file_candidate, _ = self.resolve_file_paths()
            df_prod = self.read_file(Path(input_file_baseline), file_type)
            df_qa = self.read_file(Path(input_file_candidate), file_type)

        df_prod.columns = df_prod.columns.str.strip()
        df_qa.columns = df_qa.columns.str.strip()

        key_column = self.rules_config["identifier"]

        if key_column not in df_prod.columns or key_column not in df_qa.columns:
            raise ValueError(f"Key identifier '{key_column}' not found in both datasets.")

        df_merged = df_prod.merge(
            df_qa, on=key_column, suffixes=("_baseline", "_candidate"), how="outer", indicator=True
        )

        df_merged = df_merged[df_merged["_merge"] == "both"]

        # ✅ Identify Missing Rows Before Applying Rules
        extra_rows_candidate = df_qa[~df_qa[key_column].isin(df_prod[key_column])]
        extra_rows_baseline = df_prod[~df_prod[key_column].isin(df_qa[key_column])]

        discrepancies = []

        # ✅ Apply Filters Dynamically If Provided
        for rule in self.rules_config["rules"]:
            rule_number = rule.get("Rule Number", "N/A")
            rule_type = rule["type"]
            rule_description = rule["description"]

            # ✅ Fetch updated filter values or use default values from rules_config
            updated_acceptable = filters.get(rule_number, {}).get("acceptable", rule.get("acceptable", 0))
            updated_warning_min = filters.get(rule_number, {}).get("warning_min", rule.get("warning", {}).get("min", updated_acceptable))
            updated_warning_max = filters.get(rule_number, {}).get("warning_max", rule.get("warning", {}).get("max", float("inf")))
            updated_fatal_min = filters.get(rule_number, {}).get("fatal_min", rule.get("fatal", {}).get("min", float("inf")))
            updated_threshold = filters.get(rule_number, {}).get("threshold", rule.get("threshold", 0.1))
            updated_days = filters.get(rule_number, {}).get("days", rule.get("days", 0))

            for col in rule["columns"]:
                col_baseline = f"{col}_baseline"
                col_candidate = f"{col}_candidate"

                if col_baseline in df_merged.columns and col_candidate in df_merged.columns:
                    df_merged["rule_violation"] = 0
                    df_merged["classification"] = "ACCEPTABLE"
                    is_string_column = df_merged[col_baseline].dtype == object or df_merged[col_candidate].dtype == object
                    has_only_category = "Category" in rule and not any(k in rule for k in ["threshold", "acceptable", "warning", "fatal", "days"])

                    # ✅ Apply Tolerance Rules
                    if "acceptable" in rule or "warning" in rule or "fatal" in rule:
                        df_merged["rule_violation"] = abs(pd.to_numeric(df_merged[col_candidate], errors="coerce") - pd.to_numeric(df_merged[col_baseline], errors="coerce"))
                        df_merged.loc[df_merged["rule_violation"] >= updated_fatal_min, "classification"] = "FATAL"
                        df_merged.loc[(df_merged["rule_violation"] >= updated_warning_min) & (df_merged["rule_violation"] < updated_warning_max), "classification"] = "WARNING"

                    # ✅ Apply Threshold Rules
                    elif "threshold" in rule:
                        df_merged["rule_violation"] = abs(pd.to_numeric(df_merged[col_candidate], errors="coerce") - pd.to_numeric(df_merged[col_baseline], errors="coerce")) >= updated_threshold
                        df_merged.loc[df_merged["rule_violation"], "classification"] = rule.get("Category", "None")

                    # ✅ Apply Date Rules
                    elif any("date" in col.lower() for col in rule["columns"]) or any(pd.api.types.is_datetime64_any_dtype(df_merged[col_baseline]) for col in rule["columns"]):
                        # ✅ Convert to date only (drop time part)
                        df_merged[col_baseline] = pd.to_datetime(df_merged[col_baseline], errors="coerce").dt.date
                        df_merged[col_candidate] = pd.to_datetime(df_merged[col_candidate], errors="coerce").dt.date
                        df_merged["Trade_Date_Diff"] = (pd.to_datetime(df_merged[col_candidate]) - pd.to_datetime(df_merged[col_baseline])).dt.days
                        df_merged["Trade_Date_Diff"] = df_merged["Trade_Date_Diff"].fillna(0).astype(int)
                        df_merged.loc[df_merged["Trade_Date_Diff"].abs() > updated_days, "classification"] = rule.get("Category", "None")
                    # elif any("date" in col.lower() for col in rule["columns"]) or any(pd.api.types.is_datetime64_any_dtype(df_merged[col_baseline]) for col in rule["columns"]):
                    #     df_merged["Trade_Date_Diff"] = (df_merged[col_candidate] - df_merged[col_baseline]).dt.days
                    #     df_merged["Trade_Date_Diff"] = df_merged["Trade_Date_Diff"].fillna(0).astype(int)
                    #     df_merged.loc[df_merged["Trade_Date_Diff"].abs() > updated_days, "classification"] = rule.get("Category", "None")
                    
                    elif rule_type == "ignore_differences":
                        continue
                    elif is_string_column and has_only_category:
                        category = rule.get("Category", "None")
                        df_merged[col_baseline] = df_merged[col_baseline].astype(str).str.strip()
                        df_merged[col_candidate] = df_merged[col_candidate].astype(str).str.strip()

                        df_merged["String_Mismatch"] = df_merged.apply(lambda row: row[col_baseline] != row[col_candidate], axis=1)
                        df_merged.loc[df_merged["String_Mismatch"], "classification"] = rule.get("Category", "None")

                    # ✅ Collect Discrepancies
                    for _, row in df_merged[df_merged["classification"] != "ACCEPTABLE"].iterrows():
                        discrepancies.append({
                            key_column: row[key_column],
                            "Column Name": col,
                            "Rule Type": rule_type,
                            "Category": row["classification"],
                            "Rule Number": rule_number,
                            "Description": rule_description,
                            "Baseline Field Value": row[col_baseline],
                            "Candidate Field Value": row[col_candidate]
                        })

        # ✅ Include Missing Rows
        for _, row in extra_rows_baseline.iterrows():
            discrepancies.append({
                key_column: row[key_column],
                "Column Name": "ALL",
                "Rule Type": "Missing in Candidate",
                "Category": "INFO",
                "Rule Number": "Missing_Row_Baseline",
                "Description": "Row exists in baseline but is missing in candidate.",
                "Baseline Field Value": row.to_dict(),
                "Candidate Field Value": "MISSING"
            })

        for _, row in extra_rows_candidate.iterrows():
            discrepancies.append({
                key_column: row[key_column],
                "Column Name": "ALL",
                "Rule Type": "Missing in Baseline",
                "Category": "INFO",
                "Rule Number": "Missing_Row_Candidate",
                "Description": "Row exists in candidate but is missing in baseline.",
                "Baseline Field Value": "MISSING",
                "Candidate Field Value": row.to_dict()
            })

        # Convert discrepancies to DataFrame
        discrepancies_df = pd.DataFrame(discrepancies)

        # ✅ Ensure 'Category' Column Exists
        if "Category" not in discrepancies_df.columns:
            discrepancies_df["Category"] = "UNKNOWN"

        # ✅ Convert all columns to strings to avoid serialization issues
        discrepancies_df = discrepancies_df.astype(str)

        return discrepancies_df