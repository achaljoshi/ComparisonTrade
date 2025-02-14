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

    def compare_files(self, uploaded_file_baseline=None, uploaded_file_candidate=None, file_type="Excel", header_present=True, delimiter=","):
        """Compare Baseline and Candidate files based on file type and user input for header & delimiter."""
        
        # Get delimiter from rules_config.json if available
        delimiter = self.rules_config.get("text_file_delimiter", delimiter)

        # Load files based on type
        if uploaded_file_baseline and uploaded_file_candidate:
            if file_type == "Excel (.xlsx)":
                df_baseline = pd.read_excel(uploaded_file_baseline, engine="openpyxl")
                df_candidate = pd.read_excel(uploaded_file_candidate, engine="openpyxl")
            elif file_type == "Text Files (.txt)":
                df_baseline = pd.read_csv(uploaded_file_baseline, delimiter=delimiter, header=0 if header_present else None)
                df_candidate = pd.read_csv(uploaded_file_candidate, delimiter=delimiter, header=0 if header_present else None)
        else:
            # Use preconfigured file paths
            input_file_baseline, input_file_candidate, output_file_result = self.resolve_file_paths()

            if not os.path.exists(input_file_baseline) or not os.path.exists(input_file_candidate):
                raise FileNotFoundError(f"Baseline or Candidate file not found.\n"
                                        f"Baseline: {input_file_baseline}\n"
                                        f"Candidate: {input_file_candidate}")

            if file_type == "Excel (.xlsx)":
                df_baseline = pd.read_excel(input_file_baseline, engine="openpyxl")
                df_candidate = pd.read_excel(input_file_candidate, engine="openpyxl")
            elif file_type == "Text Files (.txt)":
                df_baseline = pd.read_csv(input_file_baseline, delimiter=delimiter, header=0 if header_present else None)
                df_candidate = pd.read_csv(input_file_candidate, delimiter=delimiter, header=0 if header_present else None)

        # Determine identifier
        if file_type == "Text Files (.txt)" and not header_present:
            identifier_column = df_baseline.columns[0]  # First column is the identifier if no header
        else:
            identifier_column = self.rules_config["identifier"]

        # Ensure identifier column exists
        if identifier_column not in df_baseline.columns or identifier_column not in df_candidate.columns:
            raise ValueError(f"Key identifier '{identifier_column}' not found in both datasets.")

        # Merge Baseline and Candidate data on the key column
        df_merged = df_baseline.merge(df_candidate, on=identifier_column, suffixes=("_baseline", "_candidate"), how="outer", indicator=True)

        discrepancies = []

        # Compare each column for differences
        for col in df_baseline.columns:
            if col == identifier_column:
                continue  # Skip identifier column

            col_baseline = f"{col}_baseline"
            col_candidate = f"{col}_candidate"

            if col_baseline in df_merged.columns and col_candidate in df_merged.columns:
                df_merged[col_baseline] = df_merged[col_baseline].astype(str).str.strip()
                df_merged[col_candidate] = df_merged[col_candidate].astype(str).str.strip()

                diff_mask = df_merged[col_baseline] != df_merged[col_candidate]

                for _, row in df_merged[diff_mask].iterrows():
                    discrepancies.append({
                        "Identifier": row[identifier_column],
                        "Column": col,
                        "Baseline Value": row[col_baseline],
                        "Candidate Value": row[col_candidate],
                        "Discrepancy": "Mismatch"
                    })

        discrepancies_df = pd.DataFrame(discrepancies)

        # Ensure output directory exists before saving
        _, _, output_file_result = self.resolve_file_paths()
        output_dir = os.path.dirname(output_file_result)

        if not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)

        # Save discrepancies to output file
        discrepancies_df.to_excel(output_file_result, index=False)

        return discrepancies_df
