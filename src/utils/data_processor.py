import json  # ✅ Fix: Ensure JSON module is imported
import os
import re
from io import BytesIO
from pathlib import Path
from typing import Generator, Union
import pandas as pd


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

    def run_comparison(
        self, baseline_file, candidate_file, file_type="Excel", filters=None
    ):
        """Run the discrepancy check process with support for TXT, CSV, and Excel formats."""

        # Ensure `file_type` is always lowercase for consistency
        file_type = file_type.lower()

        # Read baseline and candidate files based on type
        if file_type == "excel":
            df_baseline = pd.read_excel(baseline_file, engine="openpyxl")
            df_candidate = pd.read_excel(candidate_file, engine="openpyxl")

        elif file_type == "csv":
            df_baseline = pd.read_csv(baseline_file)
            df_candidate = pd.read_csv(candidate_file)

        elif file_type == "txt":
            # 🔹 Instead of `pd.read_csv()`, use `read_dd_file()`
            df_baseline = pd.concat(self.read_dd_file(baseline_file), ignore_index=True)
            df_candidate = pd.concat(
                self.read_dd_file(candidate_file), ignore_index=True
            )

        else:
            raise ValueError(f"Unsupported file type: {file_type}")

        return self.compare_files(df_baseline, df_candidate, file_type, filters)

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
        input_file_baseline = Path(
            self.directory_config["input_file_baseline"].format(
                input_base_dir_baseline=self.directory_config[
                    "input_base_dir_baseline"
                ],
                ENV=baseline_env,
                DD_file_date=baseline_label,
            )
        ).resolve()

        input_file_candidate = Path(
            self.directory_config["input_file_candidate"].format(
                input_base_dir_candidate=self.directory_config[
                    "input_base_dir_candidate"
                ],
                ENV=candidate_env,
                DD_file_date=candidate_label,
            )
        ).resolve()

        output_file_result = Path(
            self.directory_config["output_file_result"].format(
                output_base_dir=self.directory_config["output_base_dir"],
                BASELINE_ENV=baseline_env,
                CANDIDATE_ENV=candidate_env,
                DD_file_date=baseline_label,
                rundate=candidate_label,
            )
        ).resolve()

        return input_file_baseline, input_file_candidate, output_file_result

    def read_dd_file(
        self, file: Union[str, BytesIO], chunk_size=500
    ) -> Generator[pd.DataFrame, None, None]:
        """Reads a structured DD file, handling nested objects & arrays dynamically while processing large files in chunks."""

        records = []
        current_record = {}
        inside_record = False
        stack = []
        current_key = None

        # Identify array and object fields based on rules_config.json
        array_fields = {
            key: rule["sub_columns"]
            for key, rules in self.rules_config["rules"].items()
            for rule in rules
            if rule.get("format_type", "").lower() == "array" and "sub_columns" in rule
        }

        # ✅ Handle both file paths and uploaded files (BytesIO)
        if isinstance(file, BytesIO):  # Handle Streamlit file uploader
            file.seek(0)  # Ensure we start reading from the beginning
            file = (
                file.read().decode("utf-8", errors="replace").splitlines()
            )  # Read as string & split into lines
        elif isinstance(file, (str, Path)):  # Handle file paths
            with open(file, "r", encoding="utf-8", errors="replace") as f:
                file = f.readlines()

        # ✅ Stream file line-by-line (efficient memory usage)
        for line in file:
            line = line.strip()

            # ✅ Start a new record
            if line == "{":
                if inside_record:
                    stack.append((current_record, current_key))
                    current_record = {}
                inside_record = True
                continue

            # ✅ End of a record or nested object
            elif line == "}":
                if stack:
                    parent_record, parent_key = stack.pop()
                    parent_record[parent_key] = (
                        current_record  # Assign nested object to its parent key
                    )
                    current_record = parent_record  # Restore previous context
                    current_key = None
                else:
                    records.append(current_record)  # Store complete record
                    current_record = {}
                    inside_record = False  # End of full record

                # ✅ Process chunk if size exceeds threshold
                if len(records) >= chunk_size:
                    yield pd.DataFrame(records)  # ✅ Yield DataFrame chunk
                    records = []  # ✅ Reset records list

                continue

            # Handle key-value pairs
            if "=" in line:
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

    def parse_nested_object(self, text):
        if not isinstance(text, str):
            return "{}"  # ✅ Ensure we return an empty object format if input is not a string

        # ✅ Extract key-value pairs from nested object
        matches = re.findall(r"(\w+)\s*=\s*([\w\d\-]+)", text)

        return "{ " + " ".join(f"{key} = {value}" for key, value in matches) + " }"

    def parse_array(self, text):
        matches = re.findall(r"\[(\d+)=([\d\w\-]+)\]", text)

        return {f"{index}": value for index, value in matches}

    def read_logs_file(
        self, file: Union[str, Path, BytesIO], file_type: str, chunk_size=500
    ) -> pd.DataFrame:
        """Reads various log-based files (.log, .csv, .txt, .json) with chunking support."""

        # ✅ Handle Streamlit `BytesIO` uploaded files
        if isinstance(file, BytesIO) or hasattr(file, "read"):
            file.seek(0)  # Reset pointer for reading
            file_name = getattr(file, "name", "uploaded_file")
            file_suffix = Path(file_name).suffix.lower()
        else:
            file_suffix = Path(file).suffix.lower()

        # ✅ Read `.log` files (pipe `|` delimited)
        if file_type == "LOG":
            return pd.concat(
                pd.read_csv(
                    file,
                    delimiter="|",
                    encoding="utf-8",
                    encoding_errors="replace",
                    chunksize=chunk_size,
                ),
                ignore_index=True,
            )

        elif file_type == "CSV":
            return pd.concat(
                pd.read_csv(
                    file,
                    encoding="utf-8",
                    encoding_errors="replace",
                    chunksize=chunk_size,
                ),
                ignore_index=True,
            )

        elif file_type == "TEXT":
            delimiter = self.rules_config.get("text_file_delimiter", ",")
            header = (
                0
                if self.rules_config.get("text_file_contains_header", "yes").lower()
                == "yes"
                else None
            )
            return pd.concat(
                pd.read_csv(
                    file,
                    delimiter=delimiter,
                    header=header,
                    encoding="utf-8",
                    encoding_errors="replace",
                    chunksize=chunk_size,
                ),
                ignore_index=True,
            )

        elif file_type == "JSON":
            try:
                data = (
                    json.load(file)
                    if isinstance(file, BytesIO)
                    else json.load(open(file, "r", encoding="utf-8"))
                )
                return (
                    pd.json_normalize(data)
                    if isinstance(data, list)
                    else pd.DataFrame([data])
                )
            except json.JSONDecodeError as e:
                raise ValueError("Invalid JSON format in file.") from e

        else:
            raise ValueError(f"Unsupported file format: {file_suffix}")

    def read_excel_file(self, file, chunk_size=500) -> pd.DataFrame:
        """Reads an Excel file efficiently using chunks (if needed)."""
        df = pd.read_excel(file, engine="openpyxl")  # ✅ Read normally
        if len(df) > chunk_size:  # ✅ If file is too large, process in chunks
            chunks = [df[i : i + chunk_size] for i in range(0, len(df), chunk_size)]
            return pd.concat(chunks, ignore_index=True)
        return df

    def read_file(self, file, file_type: str, chunk_size=500) -> pd.DataFrame:
        """Efficiently reads files using existing methods, handling large datasets with chunking."""

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

        elif isinstance(file, (str, Path)):
            file_path = Path(file)
            if file_type == "Excel" and file_path.suffix.lower() in {
                ".xlsx",
                ".xls",
            }:
                return self.read_excel_file(file_path, chunk_size)
            elif file_type == "DD":
                return pd.concat(
                    self.read_dd_file(file_path, chunk_size), ignore_index=True
                )
            elif file_type in {"TEXT", "CSV", "JSON", "LOG"}:
                return self.read_logs_file(file_path, file_type, chunk_size)
            else:
                raise ValueError(f"Unsupported file type: {file_path.suffix}")
        else:
            raise TypeError(f"Invalid file type received: {type(file)}")

    def compare_files(
        self, df_baseline=None, df_candidate=None, file_type="Excel", filters=None
    ):
        """Compare Baseline and Candidate files using dynamically defined rules from rules_config.json."""
        if filters is None:
            filters = {}

        if df_baseline is not None and df_candidate is not None:
            df_baseline, df_candidate = df_baseline.copy(), df_candidate.copy()
        else:
            input_file_baseline, input_file_candidate, _ = self.resolve_file_paths()

            # ✅ Call `read_file()` to support all formats
            df_baseline = self.read_file(
                (
                    input_file_baseline
                    if isinstance(input_file_baseline, (BytesIO))
                    else Path(input_file_baseline)
                ),
                file_type,
            )
            df_candidate = self.read_file(
                (
                    input_file_candidate
                    if isinstance(input_file_candidate, (BytesIO))
                    else Path(input_file_candidate)
                ),
                file_type,
            )

        print("Baseline Data (Before Cleaning):\n", df_baseline.head())
        print("Candidate Data (Before Cleaning):\n", df_candidate.head())

        if isinstance(self.rules_config, str):
            print(
                "🚨 ERROR: rules_config is a string instead of a dictionary! Decoding JSON..."
            )
            self.rules_config = json.loads(self.rules_config)

        if "rules" not in self.rules_config or not isinstance(
            self.rules_config["rules"], dict
        ):
            raise ValueError(
                "Error: 'rules' key not found or incorrectly formatted in rules_config.json. Please check your config file."
            )

        key_column = self.rules_config["identifier"]
        if not key_column:
            raise ValueError(
                "Error: 'messageId' key not found in rules_config.json under 'rules'. Please check your config file."
            )

        df_baseline.columns = df_baseline.columns.astype(str).str.strip()
        df_candidate.columns = df_candidate.columns.astype(str).str.strip()

        print("Baseline Columns after stripping:", df_baseline.columns.tolist())
        print("Candidate Columns after stripping:", df_candidate.columns.tolist())

        if key_column not in df_baseline.columns:
            self.get_generic_message(df_baseline, key_column)

        if key_column not in df_candidate.columns:
            self.get_generic_message(df_candidate, key_column)

        df_merged = df_baseline.merge(
            df_candidate,
            on=key_column,
            suffixes=("_baseline", "_candidate"),
            how="outer",
            indicator=True,
        )

        df_merged = df_merged[df_merged["_merge"] == "both"]

        # ✅ Identify Missing Rows Before Applying Rules
        extra_rows_candidate = df_candidate[
            ~df_candidate[key_column].isin(df_baseline[key_column])
        ]
        extra_rows_baseline = df_baseline[
            ~df_baseline[key_column].isin(df_candidate[key_column])
        ]

        discrepancies = []

        array_rule_key = next(
            (
                key
                for key, rules in self.rules_config["rules"].items()
                if any("sub_columns" in rule for rule in rules)
            ),
            None,
        )

        if array_rule_key:
            updated_discrepancies = self.get_arrays_rules(
                df_merged, discrepancies, key_column, array_rule_key
            )

        # ✅ For handling normal objects
        updated_discrepancies = self.extract_discrepancy(df_merged, discrepancies, filters, key_column)

        # ✅ Include Missing Rows
        updated_discrepancies = self.missing_row_candidate(discrepancies, extra_rows_baseline, key_column)
        updated_discrepancies = self.missing_row_baseline(discrepancies, extra_rows_candidate, key_column)

        discrepancies_df = pd.DataFrame(updated_discrepancies)
        discrepancies_df = discrepancies_df.astype(str)
        print(discrepancies_df)

        return discrepancies_df

    def get_generic_message(self, df_baseline, key_column):
        print(
            f"Error: Key identifier '{key_column}' is missing from the dataset. Columns found: {df_baseline.columns.tolist()}"
        )
        raise ValueError(f"Key identifier '{key_column}' not found in dataset.")

    def get_arrays_rules(self, df_merged, discrepancies, key_column, array_rule_key):
        array_rules = self.rules_config["rules"].get(array_rule_key, [])
        # ✅ Dynamically extract sub-columns from rules_config.json
        sub_columns = next(
            (
                rule.get("sub_columns")
                for rule in array_rules
                if "sub_columns" in rule
            ),
            [],
        )
        print(f"🔹 Extracted sub_columns: {sub_columns}")  # ✅ Debugging Output
        # ✅ For handling arrays
        min_arrays, max_arrays, predefined_values = 0, 0, set()
        discrepancies = self.get_arrays(
            df_merged,
            discrepancies,
            key_column,
            max_arrays,
            min_arrays,
            sub_columns,
            array_rule_key,
            array_rules,
        )
        return discrepancies

    def missing_row_baseline(self, discrepancies, extra_rows_candidate, key_column):
        for _, row in extra_rows_candidate.iterrows():
            discrepancies.append(
                {
                    key_column: row[key_column],
                    "Column Name": "ALL",
                    "Rule Type": "Missing in Baseline",
                    "category": "INFO",
                    "Rule Number": "Missing_Row_Candidate",
                    "Description": "Row exists in candidate but is missing in baseline.",
                    "Baseline Field Value": "MISSING",
                    "Candidate Field Value": row.to_dict(),
                }
            )
        return discrepancies

    def missing_row_candidate(self, discrepancies, extra_rows_baseline, key_column):
        for _, row in extra_rows_baseline.iterrows():
            discrepancies.append(
                {
                    key_column: row[key_column],
                    "Column Name": "ALL",
                    "Rule Type": "Missing in Candidate",
                    "category": "INFO",
                    "Rule Number": "Missing_Row_Baseline",
                    "Description": "Row exists in baseline but is missing in candidate.",
                    "Baseline Field Value": row.to_dict(),
                    "Candidate Field Value": "MISSING",
                }
            )
        return discrepancies

    def extract_discrepancy(self, df_merged, discrepancies, filters, key_column):
        for column_name, rules in self.rules_config["rules"].items():
            for rule in rules:
                if not isinstance(rule, dict):
                    raise ValueError(
                        f"Error: Rule for column '{column_name}' is not formatted correctly. Expected dict, got {type(rule)}"
                    )

                rule_number = rule.get("Rule Number", "N/A")
                rule_type = rule.get("type", "Unknown")
                rule_description = rule.get("description", "No description available")

                for col in rule.get("columns", []):

                    pattern = re.compile(r"{col}\[\d+\]")

                    if any(pattern.fullmatch(col) for col in df_merged.columns):
                        print("Columns matching 'arrays[N]' pattern exist")
                    else:
                        print("No matching columns found")

                    matching_columns = [
                        col for col in df_merged.columns if pattern.fullmatch(col)
                    ]
                    print("Matching Columns:", matching_columns)

                    col_baseline = f"{col}_baseline"
                    col_candidate = f"{col}_candidate"

                    if (
                        col_baseline in df_merged.columns
                        and col_candidate in df_merged.columns
                    ):
                        updated_constraint_min = filters.get(rule_number, {}).get(
                            "constraints_min",
                            rule.get("constraints", {}).get("min", float("inf")),
                        )
                        updated_constraint_max = filters.get(rule_number, {}).get(
                            "constraints_max",
                            rule.get("constraints", {}).get("max", float("inf")),
                        )

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
                            df_merged.loc[
                                df_merged["rule_violation"], "classification"
                            ] = rule.get("category", "None")

                        discrepancies = self.final_discrepancy_list(
                            df_merged,
                            discrepancies,
                            key_column,
                            rule_number,
                            rule_type,
                            rule_description,
                            col,
                            col_baseline,
                            col_candidate,
                        )
        return discrepancies

    def final_discrepancy_list(
        self,
        df_merged,
        discrepancies,
        key_column,
        rule_number,
        rule_type,
        rule_description,
        col,
        col_baseline,
        col_candidate,
    ):
        for _, row in df_merged[df_merged["rule_violation"] > 0].iterrows():
            discrepancies.append(
                {
                    key_column: row[key_column],
                    "Column Name": col,
                    "Rule Type": rule_type,
                    "category": row["classification"],
                    "Rule Number": rule_number,
                    "Description": rule_description,
                    "Baseline Field Value": row[col_baseline],
                    "Candidate Field Value": row[col_candidate],
                }
            )
        return discrepancies

    def get_arrays(
        self,
        df_merged,
        discrepancies,
        key_column,
        max_arrays,
        min_arrays,
        sub_columns,
        array_rule_key,
        array_rules,):
        for rule in array_rules:
            rule_number = rule.get("Rule Number", "N/A")
            rule_type = rule.get("type", "Unknown")
            rule_description = rule.get("description", "No description available")
            if "constraints" in rule:
                min_arrays = rule["constraints"].get("min", min_arrays)
                max_arrays = rule["constraints"].get("max", max_arrays)
            if "valid_values" in rule:
                predefined_values = set(rule["valid_values"])

            # ✅ Generalized regex pattern for extracting array_rule_key[]
            array_pattern = re.compile(
                r"\{\s*"
                + r"\s*".join(
                    [rf"{re.escape(col)}\s*=\s*(-?\d+)" for col in sub_columns]
                )
                + r"\s*\}"
            )

            print(
                f"🔹 Generated Regex Pattern: {array_pattern.pattern}"
            )  # ✅ Debugging Output

            if array_rule_key in self.rules_config["rules"]:
                format_type = next(
                    (
                        rule["format_type"]
                        for rule in self.rules_config["rules"][array_rule_key]
                        if "format_type" in rule
                    ),
                    None,
                )
            else:
                format_type = None

            if format_type == "Array":
                # Match columns (Array Format)
                array_columns_baseline = [
                    col
                    for col in df_merged.columns
                    if re.search(
                        rf"{re.escape(array_rule_key)}\[\d+\]_baseline", col
                    )
                ]
                array_columns_candidate = [
                    col
                    for col in df_merged.columns
                    if re.search(
                        rf"{re.escape(array_rule_key)}\[\d+\]_candidate", col
                    )
                ]
                print(
                    f"🔹 Extracted Array Columns (Baseline): {array_columns_baseline}"
                )
                print(
                    f"🔹 Extracted Array Columns (Candidate): {array_columns_candidate}"
                )

            elif format_type == "Object":
                # Match matchId columns (Object Format)
                array_columns_baseline = [
                    col
                    for col in df_merged.columns
                    if re.search(rf"{re.escape(array_rule_key)}_baseline", col)
                ]
                array_columns_candidate = [
                    col
                    for col in df_merged.columns
                    if re.search(rf"{re.escape(array_rule_key)}_candidate", col)
                ]
                print(
                    f"🔹 Extracted Object Columns (Baseline): {array_columns_baseline}"
                )
                print(
                    f"🔹 Extracted Object Columns (Candidate): {array_columns_candidate}"
                )

            else:
                print(
                    f"⚠️ Warning: Format type unknown for {array_rule_key}, no extraction applied."
                )
                array_columns_baseline, array_columns_candidate = [], []

            print(
                f"🔹 Extracted array_rule_key columns (Baseline): {array_columns_baseline}"
            )
            print(
                f"🔹 Extracted array_rule_key columns (Candidate): {array_columns_candidate}"
            )

            # ✅ Iterate over all rows
            for _, row in df_merged.iterrows():
                if format_type == "Array":
                    baseline_str = " ".join(
                        str(row[col])
                        for col in array_columns_baseline
                        if pd.notna(row[col])
                    )
                    candidate_str = " ".join(
                        str(row[col])
                        for col in array_columns_candidate
                        if pd.notna(row[col])
                    )

                elif format_type == "Object":
                    baseline_str = (
                        str(row[array_columns_baseline[0]])
                        if array_columns_baseline
                        else ""
                    )
                    candidate_str = (
                        str(row[array_columns_candidate[0]])
                        if array_columns_candidate
                        else ""
                    )

                # ✅ Debugging output
                print(f"\nRow Index: {row.name}")
                print("Baseline arrays Raw String:", baseline_str)
                print("Candidate arrays Raw String:", candidate_str)

                # ✅ Extract matches using regex
                baseline_matches = array_pattern.findall(baseline_str)
                candidate_matches = array_pattern.findall(candidate_str)

                print("Regex Matches for Baseline:", baseline_matches)
                print("Regex Matches for Candidate:", candidate_matches)

                # ✅ Convert extracted matches into dictionaries
                baseline_arrays = {
                    idx: {sub_columns[i]: int(m[i]) for i in range(len(sub_columns))}
                    for idx, m in enumerate(baseline_matches)
                    if len(m) == len(sub_columns)
                }
                candidate_arrays = {
                    idx: {sub_columns[i]: int(m[i]) for i in range(len(sub_columns))}
                    for idx, m in enumerate(candidate_matches)
                    if len(m) == len(sub_columns)
                }

                # ✅ Iterate over all array_rule_key indices from both datasets -- for arrays
                for idx in set(baseline_arrays.keys()).union(
                    candidate_arrays.keys()
                ):
                    base_values = baseline_arrays.get(idx, {})
                    cand_values = candidate_arrays.get(idx, {})

                    for field in sub_columns:  # ✅ Iterate dynamically over sub_columns

                        if len(baseline_arrays) > 1 and len(candidate_arrays) > 1:
                            output_column_name = f"{array_rule_key}[{idx}].{field}"
                        else:
                            output_column_name = f"{array_rule_key}.{field}"

                        base_val = base_values.get(field)
                        cand_val = cand_values.get(field)

                        if "constraints" in rule:
                            df_merged["rule_violation"] = abs(
                                pd.to_numeric(base_val) - pd.to_numeric(cand_val)
                            )
                            self._extracted_discrepancy_classification(
                                rule, df_merged, min_arrays, max_arrays
                            )
                        discrepancies = self.discrepancy_list(
                            df_merged,
                            discrepancies,
                            key_column,
                            rule_number,
                            rule_type,
                            rule_description,
                            output_column_name,
                            base_val,
                            cand_val,
                        )
        return discrepancies
    

    def discrepancy_list(
        self,
        df_merged,
        discrepancies,
        key_column,
        rule_number,
        rule_type,
        rule_description,
        output_column_name,
        base_val,
        cand_val,
    ):
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
        return discrepancies

    def _extracted_discrepancy_classification(self, rule, df_merged, arg2, arg3):
        df_merged.loc[df_merged["rule_violation"] <= arg2, "classification"] = rule.get(
            "category", "ACCEPTABLE"
        )
        df_merged.loc[df_merged["rule_violation"] >= arg3, "classification"] = rule.get(
            "category", "FATAL"
        )
        df_merged.loc[
            (df_merged["rule_violation"] > arg2) & (df_merged["rule_violation"] < arg3),
            "classification",
        ] = rule.get("category", "WARNING")
