import json  # ✅ Fix: Ensure JSON module is imported
import os
import numpy as np
import pandas as pd
from datetime import datetime, timedelta


class DataProcessor:
    def __init__(self, directory_config_path=None, job_response_path=None, rules_config_path=None):
        """Load directory paths, job details, and validation rules."""
        self.directory_config_path = directory_config_path
        self.job_response_path = job_response_path
        self.rules_config_path = rules_config_path

        # Load configuration files
        with open(self.directory_config_path, "r") as file:
            self.directory_config = json.load(file)

        with open(self.job_response_path, "r") as file:
            self.job_response = json.load(file)

        with open(self.rules_config_path, "r") as file:
            self.rules_config = json.load(file)

    def resolve_file_paths(self):
        """Resolve actual file paths using job response and directory config."""
        baseline_env = self.job_response["baseline"]["env"]
        baseline_label = self.job_response["baseline"]["label"]
        candidate_env = self.job_response["candidate"]["env"]
        candidate_label = self.job_response["candidate"]["label"]

        # Resolve file paths
        input_file_baseline = self.directory_config["input_file_baseline"].format(
            input_base_dir_baseline=self.directory_config["input_base_dir_baseline"],
            ENV=baseline_env,
            DD_file_date=baseline_label
        )

        input_file_candidate = self.directory_config["input_file_candidate"].format(
            input_base_dir_candidate=self.directory_config["input_base_dir_candidate"],
            ENV=candidate_env,
            DD_file_date=candidate_label
        )

        output_file_result = self.directory_config["output_file_result"].format(
            output_base_dir=self.directory_config["output_base_dir"],
            BASELINE_ENV=baseline_env,
            CANDIDATE_ENV=candidate_env,
            DD_file_date=baseline_label,
            rundate=candidate_label
        )

        return input_file_baseline, input_file_candidate, output_file_result
  

    def compare_files(self, df_baseline=None, df_candidate=None, file_type="Excel"):
        """Compare Baseline and Candidate files using dynamically defined rules from rules_config.json."""

        # ✅ **Use Uploaded DataFrames If Available**
        if df_baseline is not None and df_candidate is not None:
            print("✅ Using uploaded DataFrames for comparison.")
            df_prod, df_qa = df_baseline.copy(), df_candidate.copy()  # Copy to avoid modifying original data
        else:
            print("⚠️ No uploaded DataFrames found. Using preconfigured file paths.")
            input_file_baseline, input_file_candidate, output_file_result = self.resolve_file_paths()

            if not os.path.exists(input_file_baseline) or not os.path.exists(input_file_candidate):
                raise FileNotFoundError(f"❌ Baseline or Candidate file not found.\n"
                                        f"Baseline: {input_file_baseline}\n"
                                        f"Candidate: {input_file_candidate}")

            print("✅ Using preconfigured file paths for comparison.")
            if file_type == "Excel":
                df_prod = pd.read_excel(input_file_baseline, engine="openpyxl")
                df_qa = pd.read_excel(input_file_candidate, engine="openpyxl")
            elif file_type in ["Text Files", "Datadog Logs"]:
                delimiter = self.rules_config.get("text_file_delimiter", ",")
                df_prod = pd.read_csv(input_file_baseline, delimiter=delimiter, header=0)
                df_qa = pd.read_csv(input_file_candidate, delimiter=delimiter, header=0)

        # ✅ **Ensure Files Are Successfully Loaded Before Proceeding**
        if df_prod is None or df_qa is None:
            raise ValueError("❌ Failed to load data for comparison. Please check file paths and formats.")

        # ✅ Standardize Column Names
        df_prod.columns = df_prod.columns.str.strip()
        df_qa.columns = df_qa.columns.str.strip()

        # ✅ Identify Common Key Column for Merging
        key_column = self.rules_config["identifier"]

        if key_column not in df_prod.columns or key_column not in df_qa.columns:
            raise ValueError(f"Key identifier '{key_column}' not found in both datasets.")

        # Ensure TradeDate is converted to datetime before merging
        if "TradeDate" in df_prod.columns and "TradeDate" in df_qa.columns:
            df_prod["TradeDate"] = pd.to_datetime(df_prod["TradeDate"].astype(str).str.strip(), errors="coerce")
            df_qa["TradeDate"] = pd.to_datetime(df_qa["TradeDate"].astype(str).str.strip(), errors="coerce")

        # ✅ Merge after fixing spaces, keeping all data
        df_merged = df_prod.merge(
            df_qa, on=key_column, suffixes=("_baseline", "_candidate"), how="outer", indicator=True
        )

        df_merged = df_merged[df_merged["_merge"] == "both"]


        # ✅ Identify Missing Rows in Baseline & Candidate
        extra_rows_candidate = df_qa[~df_qa[key_column].isin(df_prod[key_column])]
        extra_rows_baseline = df_prod[~df_prod[key_column].isin(df_qa[key_column])]

        discrepancies = []

        # ✅ Apply All Rules Dynamically & Log Separately
        for rule in self.rules_config["rules"]:
            rule_type = rule["type"]
            rule_number = rule.get("Rule Number", "N/A")
            rule_description = rule["description"]

            rule_discrepancies = []  # ✅ Separate log for each rule

            for col in rule["columns"]:
                col_baseline = f"{col}_baseline"
                col_candidate = f"{col}_candidate"

                if col_baseline in df_merged.columns and col_candidate in df_merged.columns:
                    # ✅ Convert to Numeric if Necessary ----- Unnecessary code
                    # df_merged[col_baseline] = pd.to_numeric(df_merged[col_baseline], errors="coerce")
                    # df_merged[col_candidate] = pd.to_numeric(df_merged[col_candidate], errors="coerce")

                    # ✅ Ensure `rule_violation` Column Exists & Fill NaN
                    df_merged["rule_violation"] = 0  # Initialize as 0
                    df_merged["classification"] = "ACCEPTABLE"  # Default Category

                    # ✅ Tolerance-Based Rules (Acceptable, Warning, Fatal)
                    if "acceptable" in rule or "warning" in rule or "fatal" in rule:
                        acceptable = rule.get("acceptable", 0)
                        warning_min = rule.get("warning", {}).get("min", acceptable)
                        warning_max = rule.get("warning", {}).get("max", float("inf"))
                        fatal_min = rule.get("fatal", {}).get("min", float("inf"))

                        df_merged["rule_violation"] = abs(df_merged[col_candidate] - df_merged[col_baseline])
                        df_merged.loc[df_merged["rule_violation"] >= fatal_min, "classification"] = "FATAL"
                        df_merged.loc[(df_merged["rule_violation"] >= warning_min) & (df_merged["rule_violation"] < warning_max), "classification"] = "WARNING"
                        
                        # ✅ **Collect Discrepancies for Tolerance-Based Rules**
                        for _, row in df_merged[df_merged["rule_violation"] > 0].iterrows():
                            discrepancies.append({
                                "Identifier": row[key_column],
                                "Column Name": col,  # ✅ Include actual column name in discrepancies
                                "Rule Type": rule_type,
                                "Category": row["classification"],  # ✅ Uses correct category assignment
                                "Rule Number": rule_number,
                                "Description": rule_description,
                                "Baseline Field Value": row[col_baseline],
                                "Candidate Field Value": row[col_candidate]
                            })

                    # ✅ Threshold-Based Rules
                    elif "threshold" in rule:
                        category = rule.get("Category", "None")
                        threshold = rule["threshold"]
                        df_merged["rule_violation"] = abs(df_merged[col_candidate] - df_merged[col_baseline]) >= threshold
                        df_merged.loc[df_merged["rule_violation"], "classification"] = category
                        
                        # ✅ **Collect Discrepancies for Threshold-Based Rules**
                        for _, row in df_merged[df_merged["rule_violation"] >= threshold].iterrows():
                            discrepancies.append({
                                "Identifier": row[key_column],
                                "Column Name": col,  # ✅ Include actual column name in discrepancies
                                "Rule Type": rule_type,
                                "Category": row["classification"],  # ✅ Uses predefined category
                                "Rule Number": rule_number,
                                "Description": rule_description,
                                "Baseline Field Value": row[col_baseline],
                                "Candidate Field Value": row[col_candidate]
                            })
                    
                    elif any("date" in col.lower() for col in rule["columns"]) or any(pd.api.types.is_datetime64_any_dtype(df_merged[col_baseline]) for col in rule["columns"]):
                        category = rule.get("Category", "None")

                        # ✅ Convert date/datetime columns to date format
                        df_merged[col_baseline] = pd.to_datetime(df_merged[col_baseline], errors="coerce").dt.date
                        df_merged[col_candidate] = pd.to_datetime(df_merged[col_candidate], errors="coerce").dt.date

                        # ✅ Calculate difference in days and explicitly convert to integer
                        df_merged["Trade_Date_Diff"] = (pd.to_datetime(df_merged[col_candidate]) - pd.to_datetime(df_merged[col_baseline])).dt.days
                        df_merged["Trade_Date_Diff"] = df_merged["Trade_Date_Diff"].fillna(0).astype(int)

                        # ✅ Assign the integer value to rule_violation to avoid type mismatch
                        df_merged["rule_violation"] = df_merged["Trade_Date_Diff"].astype(int)

                        # ✅ Ensure rule["acceptable"] is treated as integer
                        acceptable_threshold = int(rule["days"])

                        # ✅ Find discrepancies (date difference > threshold OR missing values)
                        trade_date_violations = df_merged[
                            (df_merged["rule_violation"].abs() > acceptable_threshold) |  # Convert threshold to int
                            df_merged[col_baseline].isna() | 
                            df_merged[col_candidate].isna()
                        ]

                        # ✅ Collect discrepancies
                        for _, row in trade_date_violations.iterrows():
                            discrepancies.append({
                                "Identifier": row[key_column],
                                "Column Name": col,
                                "Rule Type": rule_type,
                                "Category": category,
                                "Rule Number": rule_number,
                                "Description": rule_description,
                                "Baseline Field Value": row[col_baseline] if pd.notna(row[col_baseline]) else "MISSING",
                                "Candidate Field Value": row[col_candidate] if pd.notna(row[col_candidate]) else "MISSING"
                            })
                    elif rule_type == "ignore_differences":
                        continue
                    else:
                        category = rule.get("Category", "None")
                        df_merged[col_baseline] = df_merged[col_baseline].astype(str).str.strip()
                        df_merged[col_candidate] = df_merged[col_candidate].astype(str).str.strip()

                        df_merged["String_Mismatch"] = df_merged[col_baseline] != df_merged[col_candidate]
                        string_violations = df_merged[df_merged["String_Mismatch"]]
                        print(f"Found {len(string_violations)} mismatches in {col}")

                        for _, row in string_violations.iterrows():
                            discrepancies.append({
                                "Identifier": row[key_column],
                                "Column Name": col,
                                "Rule Type": rule_type,
                                "Category": category,
                                "Rule Number": rule_number,
                                "Description": rule_description,
                                "Baseline Field Value": row[col_baseline] if row[col_baseline] else "MISSING",
                                "Candidate Field Value": row[col_candidate] if row[col_candidate] else "MISSING"
                            })

        # ✅ Include Extra Rows from Baseline
        for _, row in extra_rows_baseline.iterrows():
            discrepancies.append({
                "Identifier": row[key_column],
                "Column Name": "ALL",
                "Rule Type": "Missing in Candidate",
                "Category": "INFO",
                "Rule Number": "Missing_Row_Baseline",
                "Description": "Row exists in baseline but is missing in candidate.",
                "Baseline Field Value": row.to_dict(),
                "Candidate Field Value": "MISSING"
            })

        # ✅ Include Extra Rows from Candidate
        for _, row in extra_rows_candidate.iterrows():
            discrepancies.append({
                "Identifier": row[key_column],
                "Column Name": "ALL",
                "Rule Type": "Missing in Baseline",
                "Category": "INFO",
                "Rule Number": "Missing_Row_Candidate",
                "Description": "Row exists in candidate but is missing in baseline.",
                "Baseline Field Value": "MISSING",
                "Candidate Field Value": row.to_dict()
            })

        # Convert all dictionary-like values to strings to avoid PyArrow serialization issues
        discrepancies_df = pd.DataFrame(discrepancies)

        # for col in ["Baseline Value", "Candidate Value"]:
        #     if col in discrepancies_df.columns:
        #         discrepancies_df[col] = discrepancies_df[col].apply(lambda x: json.dumps(x) if isinstance(x, dict) else x)

        # # Ensure all columns are properly formatted for Arrow serialization
        # discrepancies_df = discrepancies_df.astype(str)

        # return discrepancies_df

        

        # ✅ Ensure 'Category' Column Exists (Fixes Missing 'Warning' Error)
        if "Category" not in discrepancies_df.columns:
            discrepancies_df["Category"] = "UNKNOWN"  # Assign a default category if missing

        # ✅ Convert all columns to strings to avoid PyArrow serialization issues
        discrepancies_df = discrepancies_df.astype(str)

        return discrepancies_df