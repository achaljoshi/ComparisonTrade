import json  # ✅ Fix: Ensure JSON module is imported
import os
import numpy as np
import pandas as pd
import re
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
        """Reads a structured text file and extracts object-based and array-based fields dynamically."""

        records = []
        current_record = {}
        inside_record = False  # Track if inside `{}` block

        # Identify array and object fields based on rules_config.json
        array_fields = {
            key: rule["sub_columns"]
            for key, rules in self.rules_config["rules"].items()
            for rule in rules
            if rule.get("format_type") == "Array" and "sub_columns" in rule
        }

        object_fields = {
            key: rule["sub_columns"]
            for key, rules in self.rules_config["rules"].items()
            for rule in rules
            if rule.get("format_type") == "Object" and "sub_columns" in rule
        }

        print(f"🔹 Identified Array Fields: {array_fields}")
        print(f"🔹 Identified Object Fields: {object_fields}")

        with open(file_path, "r") as file:
            for line in file:
                line = line.strip()

                if line == "{":
                    inside_record = True
                    current_record = {}
                    continue

                elif line == "}":
                    inside_record = False
                    if current_record:
                        records.append(current_record)
                    current_record = {}
                    continue

                if inside_record and "=" in line:
                    key, value = map(str.strip, line.split("=", 1))

                    # Handle Object-based fields
                    object_match = re.match(r"(\w+)\s*=\s*\{(.*)\}", line)
                    if object_match:
                        obj_key, obj_values = object_match.groups()
                        obj_values = obj_values.strip().split()

                        if obj_key in object_fields:
                            sub_cols = object_fields[obj_key]
                            obj_data = {}

                            # Ensure we don't exceed expected sub_columns
                            for i, sub_col in enumerate(sub_cols):
                                if i < len(obj_values) and "=" in obj_values[i]:
                                    sub_key, sub_value = obj_values[i].split("=", 1)
                                    obj_data[sub_key] = sub_value
                            
                            current_record[obj_key] = obj_data
                        continue

                    # Handle Array-based fields
                    array_match = re.match(r"(\w+)\s*=\s*(\[.*\])", line)
                    if array_match:
                        arr_key, arr_values = array_match.groups()

                        if arr_key in array_fields:
                            array_data = {
                                int(k): int(v) for k, v in re.findall(r"\[(\d+)=(\d+)\]", arr_values)
                            }
                            current_record[arr_key] = array_data
                        continue

                    # Default key-value storage
                    current_record[key] = value

        # Convert parsed records into DataFrame
        df = pd.DataFrame(records)

        print("🔹 Parsed DataFrame:\n", df.head())
        print("🔹 Columns after parsing:", df.columns.tolist())

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

        print("Baseline Data (Before Cleaning):\n", df_baseline.head())
        print("Candidate Data (Before Cleaning):\n", df_candidate.head())

        if isinstance(self.rules_config, str):
            print("🚨 ERROR: rules_config is a string instead of a dictionary! Decoding JSON...")
            self.rules_config = json.loads(self.rules_config)

        if "rules" not in self.rules_config or not isinstance(self.rules_config["rules"], dict):
            raise ValueError("Error: 'rules' key not found or incorrectly formatted in rules_config.json. Please check your config file.")

        key_column = self.rules_config["identifier"]
        if not key_column:
            raise ValueError("Error: 'messageId' key not found in rules_config.json under 'rules'. Please check your config file.")

        df_baseline.columns = df_baseline.columns.astype(str).str.strip()
        df_candidate.columns = df_candidate.columns.astype(str).str.strip()

        print("Baseline Columns after stripping:", df_baseline.columns.tolist())
        print("Candidate Columns after stripping:", df_candidate.columns.tolist())

        if key_column not in df_baseline.columns:
            print(f"Error: Key identifier '{key_column}' is missing from the Baseline dataset. Columns found: {df_baseline.columns.tolist()}")
            raise ValueError(f"Key identifier '{key_column}' not found in Baseline dataset.")

        if key_column not in df_candidate.columns:
            print(f"Error: Key identifier '{key_column}' is missing from the Candidate dataset. Columns found: {df_candidate.columns.tolist()}")
            raise ValueError(f"Key identifier '{key_column}' not found in Candidate dataset.")

        df_merged = df_baseline.merge(df_candidate, on=key_column, suffixes=("_baseline", "_candidate"), how="outer", indicator=True)

        discrepancies = []
        
       # ✅ Dynamically extract tick size rule key from rules_config.json
        ticksize_rule_key = next(
            (key for key, rules in self.rules_config["rules"].items() if any("sub_columns" in rule for rule in rules)), None
        )

        if not ticksize_rule_key:
            raise ValueError("❌ No rule found with 'sub_columns' in rules_config.json!")

        ticksize_rules = self.rules_config["rules"].get(ticksize_rule_key, [])

        # ✅ Dynamically extract sub-columns from rules_config.json
        sub_columns = next((rule.get("sub_columns") for rule in ticksize_rules if "sub_columns" in rule), [])

        # ✅ Ensure at least a default set is present
        if not sub_columns:
            sub_columns = ["lowerLimit", "upperLimit", "tickSize"]  # Default fallback

        print(f"🔹 Extracted sub_columns: {sub_columns}")  # ✅ Debugging Output

        # ✅ Extract constraints and predefined values dynamically
        min_tick_size, max_tick_size, predefined_values = 1, 9999, set()
        for rule in ticksize_rules:
            if "constraints" in rule:
                min_tick_size = rule["constraints"].get("min", min_tick_size)
                max_tick_size = rule["constraints"].get("max", max_tick_size)
            if "valid_values" in rule:
                predefined_values = set(rule["valid_values"])

        # ✅ Generalized regex pattern for extracting ticksize_rule_key[]
        ticksize_pattern = re.compile(
            r"\{\s*" + r"\s*".join([rf"{re.escape(col)}\s*=\s*(-?\d+)" for col in sub_columns]) + r"\s*\}"
        )

        print(f"🔹 Generated Regex Pattern: {ticksize_pattern.pattern}")  # ✅ Debugging Output

        # ✅ Extract all dynamic ticksize_rule_key columns dynamically
        ticksize_columns_baseline = [col for col in df_merged.columns if re.match(fr"{re.escape(ticksize_rule_key)}\[\d+\]_baseline", col)]
        ticksize_columns_candidate = [col for col in df_merged.columns if re.match(fr"{re.escape(ticksize_rule_key)}\[\d+\]_candidate", col)]

        print(f"🔹 Extracted ticksize_rule_key columns (Baseline): {ticksize_columns_baseline}")
        print(f"🔹 Extracted ticksize_rule_key columns (Candidate): {ticksize_columns_candidate}")

        # ✅ Iterate over all rows
        for _, row in df_merged.iterrows():
            # ✅ Dynamically build ticksize_rule_key string from identified columns
            baseline_str = " ".join(str(row[col]) for col in ticksize_columns_baseline if pd.notna(row[col]))
            candidate_str = " ".join(str(row[col]) for col in ticksize_columns_candidate if pd.notna(row[col]))

            # ✅ Debugging output
            print(f"\nRow Index: {row.name}")
            print("Baseline TickSizes Raw String:", baseline_str)
            print("Candidate TickSizes Raw String:", candidate_str)

            # ✅ Extract matches using regex
            baseline_matches = ticksize_pattern.findall(baseline_str)
            candidate_matches = ticksize_pattern.findall(candidate_str)

            print("Regex Matches for Baseline:", baseline_matches)
            print("Regex Matches for Candidate:", candidate_matches)

            # ✅ Convert extracted matches into dictionaries
            baseline_tickSizes = {idx: {sub_columns[i]: int(m[i]) for i in range(len(sub_columns))} for idx, m in enumerate(baseline_matches) if len(m) == len(sub_columns)}
            candidate_tickSizes = {idx: {sub_columns[i]: int(m[i]) for i in range(len(sub_columns))} for idx, m in enumerate(candidate_matches) if len(m) == len(sub_columns)}

            # ✅ Iterate over all ticksize_rule_key indices from both datasets
            for idx in set(baseline_tickSizes.keys()).union(candidate_tickSizes.keys()):
                base_values = baseline_tickSizes.get(idx, {})
                cand_values = candidate_tickSizes.get(idx, {})

                for field in sub_columns:  # ✅ Iterate dynamically over sub_columns
                    base_val = base_values.get(field)
                    cand_val = cand_values.get(field)

                    # ✅ Log discrepancies for mismatched values
                    if base_val is not None and cand_val is not None and base_val != cand_val:
                        discrepancies.append({
                            key_column: row[key_column],
                            "Column Name": f"{ticksize_rule_key}[{idx}].{field}",
                            "Rule Type": "Range check",
                            "category": "WARNING",
                            "Rule Number": "1",
                            "Description": f"Ensure that {ticksize_rule_key}[{idx}] {field} follows the defined constraints.",
                            "Baseline Field Value": base_val,
                            "Candidate Field Value": cand_val
                        })

                    # ✅ Dynamically determine the tick size field
                    tick_size_field = next((col for col in sub_columns if "tick" in col.lower()), "tickSize")

                    # ✅ Validate tickSize numerical constraints
                    if field == tick_size_field and cand_val is not None:
                        if not (min_tick_size <= cand_val <= max_tick_size):
                            discrepancies.append({
                                key_column: row[key_column],
                                "Column Name": f"{ticksize_rule_key}[{idx}].{tick_size_field}",
                                "Rule Type": "Numerical check",
                                "category": "ERROR",
                                "Rule Number": "2",
                                "Description": f"{tick_size_field} must be between {min_tick_size} and {max_tick_size}.",
                                "Baseline Field Value": base_val,
                                "Candidate Field Value": cand_val
                            })

                        # ✅ Validate tickSize predefined values
                        if predefined_values and cand_val not in predefined_values:
                            discrepancies.append({
                                key_column: row[key_column],
                                "Column Name": f"{ticksize_rule_key}[{idx}].{tick_size_field}",
                                "Rule Type": "Predefined values check",
                                "category": "ERROR",
                                "Rule Number": "3",
                                "Description": f"{tick_size_field} must be one of the predefined values: {list(predefined_values)}",
                                "Baseline Field Value": base_val,
                                "Candidate Field Value": cand_val
                            })

        for column_name, rules in self.rules_config["rules"].items():
            for rule in rules:
                if not isinstance(rule, dict):
                    raise ValueError(f"Error: Rule for column '{column_name}' is not formatted correctly. Expected dict, got {type(rule)}")

                rule_number = rule.get("Rule Number", "N/A")
                rule_type = rule.get("type", "Unknown")
                rule_description = rule.get("description", "No description available")

                for col in rule.get("columns", []):
                    col_baseline = f"{col}_baseline"
                    col_candidate = f"{col}_candidate"

                    if col_baseline in df_merged.columns and col_candidate in df_merged.columns:
                        updated_warning_min = filters.get(rule_number, {}).get("constraints_min", rule.get("constraints", {}).get("min", float("inf")))
                        updated_warning_max = filters.get(rule_number, {}).get("constraints_max", rule.get("constraints", {}).get("max", float("inf")))

                        if "constraints" in rule:
                            df_merged["rule_violation"] = abs(pd.to_numeric(df_merged[col_candidate], errors="coerce") - pd.to_numeric(df_merged[col_baseline], errors="coerce"))
                            df_merged.loc[df_merged["rule_violation"] >= updated_warning_max, "classification"] = rule.get("category", "FATAL")
                            df_merged.loc[(df_merged["rule_violation"] >= updated_warning_min) & (df_merged["rule_violation"] < updated_warning_max), "classification"] = rule.get("category", "WARNING")

                        for _, row in df_merged[df_merged["classification"] != "ACCEPTABLE"].iterrows():
                            discrepancies.append({
                                key_column: row[key_column],
                                "Column Name": col,
                                "Rule Type": rule_type,
                                "category": row["classification"],
                                "Rule Number": rule_number,
                                "Description": rule_description,
                                "Baseline Field Value": row[col_baseline],
                                "Candidate Field Value": row[col_candidate]
                            })

        discrepancies_df = pd.DataFrame(discrepancies)
        discrepancies_df = discrepancies_df.astype(str)
        print(discrepancies_df)

        return discrepancies_df
