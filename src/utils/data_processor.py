import json  # ✅ Fix: Ensure JSON module is imported
import os
import pandas as pd

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
        """Compare Baseline and Candidate files, using uploaded DataFrames if provided."""

        # ✅ **Use Uploaded DataFrames If Available**
        if df_baseline is not None and df_candidate is not None:
            print("✅ Using uploaded DataFrames for comparison.")
            df_prod, df_qa = df_baseline, df_candidate  # Assign DataFrames directly
        else:
            print("⚠️ No uploaded DataFrames found. Using preconfigured file paths.")

            # ✅ **Fallback to Preconfigured File Paths If No Uploaded DataFrames**
            input_file_baseline, input_file_candidate, output_file_result = self.resolve_file_paths()

            # **Check if preconfigured files exist before loading**
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

        print("✅ Data successfully loaded. Proceeding with standardization and comparison.")

        # ✅ **Standardize Column Names**
        df_prod.columns = df_prod.columns.str.strip()
        df_qa.columns = df_qa.columns.str.strip()

        # ✅ **Identify Common Key Column for Merging**
        key_column = self.rules_config["identifier"]

        if key_column not in df_prod.columns or key_column not in df_qa.columns:
            raise ValueError(f"Key identifier '{key_column}' not found in both datasets.")

        # ✅ **Merge Data Based on Identifier**
        df_merged = df_prod.merge(df_qa, on=key_column, suffixes=("_baseline", "_candidate"), how="inner")

        discrepancies = []

        # ✅ **Compare Each Column for Differences Based on Rules**
        for rule in self.rules_config["rules"]:
            for col in rule["columns"]:
                col_baseline = f"{col}_baseline"
                col_candidate = f"{col}_candidate"

                if col_baseline in df_merged.columns and col_candidate in df_merged.columns:
                    # ✅ **Convert to Numeric if Necessary**
                    df_merged[col_baseline] = pd.to_numeric(df_merged[col_baseline], errors="coerce")
                    df_merged[col_candidate] = pd.to_numeric(df_merged[col_candidate], errors="coerce")

                    # ✅ **Compute Absolute Difference**
                    df_merged["diff"] = abs(df_merged[col_baseline] - df_merged[col_candidate])

                    classification = "Acceptable"
                    if rule["type"] == "tolerance_check":
                        acceptable = rule.get("acceptable", 0)
                        warning_range = rule.get("warning", {})
                        fatal_threshold = rule.get("fatal", {}).get("min", float("inf"))

                        # ✅ **Determine Classification Based on Rules**
                        max_diff = df_merged["diff"].max() if "diff" in df_merged.columns else 0
                        if max_diff > fatal_threshold:
                            classification = "FATAL"
                        elif max_diff > warning_range.get("max", fatal_threshold):
                            classification = "WARNING"
                        elif max_diff > acceptable:
                            classification = "ERROR"

                    # ✅ **Collect Discrepancies**
                    for _, row in df_merged[df_merged["diff"] > 0].iterrows():
                        discrepancies.append({
                            "Identifier": row[key_column],
                            "Category": classification,
                            "Rule Number": rule.get("Rule Number", "N/A"),
                            "Description": rule["description"],
                            "Baseline Field Value": row[col_baseline],
                            "Candidate Field Value": row[col_candidate]
                        })

        # ✅ **Convert to DataFrame**
        discrepancies_df = pd.DataFrame(discrepancies)

        # ✅ **Ensure Output Directory Exists Before Saving**
        _, _, output_file_result = self.resolve_file_paths()
        output_dir = os.path.dirname(output_file_result)

        if not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)

        # ✅ **Save Discrepancies to Output File**
        discrepancies_df.to_excel(output_file_result, index=False)

        return discrepancies_df

