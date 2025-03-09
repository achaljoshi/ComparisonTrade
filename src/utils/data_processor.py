import json  # ✅ Fix: Ensure JSON module is imported
import os
import re
from io import BytesIO
from pathlib import Path
from typing import Generator, Union, Dict, Any
import pandas as pd
import logging


class DataProcessor:
    def __init__(self, directory_config, job_response, rules_config):
        """Initialize with file paths or direct dictionary data.
        
        Args:
            directory_config: Either a file path or a dictionary containing directory configuration
            job_response: Either a file path or a dictionary containing job response data
            rules_config: Either a file path or a dictionary containing rules configuration
        """
        # Handle directory config
        if isinstance(directory_config, str) and os.path.exists(directory_config):
            with open(directory_config, "r") as file:
                self.directory_config = json.load(file)
        elif isinstance(directory_config, dict):
            self.directory_config = directory_config
        else:
            raise ValueError("directory_config must be either a valid file path or a dictionary")

        # Handle job response
        if isinstance(job_response, str) and os.path.exists(job_response):
            with open(job_response, "r") as file:
                self.job_response = json.load(file)
        elif isinstance(job_response, dict):
            self.job_response = job_response
        else:
            raise ValueError("job_response must be either a valid file path or a dictionary")

        # Handle rules config
        if isinstance(rules_config, str) and os.path.exists(rules_config):
            with open(rules_config, "r") as file:
                self.rules_config = json.load(file)
        elif isinstance(rules_config, dict):
            self.rules_config = rules_config
        else:
            raise ValueError("rules_config must be either a valid file path or a dictionary")

        # Initialize other attributes
        self._initialize_attributes()

    def _initialize_attributes(self):
        """Initialize additional attributes needed for data processing."""
        try:
            # Validate rules configuration
            if not isinstance(self.rules_config, dict):
                raise ValueError("rules_config must be a dictionary")
            
            if "rules" not in self.rules_config:
                raise ValueError("rules_config must contain a 'rules' key")
            
            if "identifier" not in self.rules_config:
                raise ValueError("rules_config must contain an 'identifier' key")
            
            # Initialize processing attributes
            self.key_column = self.rules_config["identifier"]
            self.rules = self.rules_config["rules"]
            
            # Initialize filter-related attributes
            self.filter_metadata = None
            self.processed_filters = {}
            
            # Initialize comparison attributes
            self.comparison_results = None
            self.discrepancies = []
            
            logging.debug("DataProcessor attributes initialized successfully")
            
        except Exception as e:
            logging.error(f"Error initializing DataProcessor attributes: {str(e)}")
            raise

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
            key: rule["nested_identifier"]
            for key, rules in self.rules_config["rules"].items()
            for rule in rules
            if rule.get("format_type", "").lower() == "array" and "nested_identifier" in rule
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
                if any("nested_identifier" in rule for rule in rules)
            ),
            None,
        )

        if array_rule_key:
            # Pass filters to get_arrays_rules
            updated_discrepancies = self.get_arrays_rules(
                df_merged, discrepancies, key_column, array_rule_key, filters
            )
        else:
            updated_discrepancies = discrepancies

        # ✅ For handling normal objects
        updated_discrepancies = self.extract_discrepancy(df_merged, updated_discrepancies, filters, key_column)

        # ✅ Include Missing Rows
        updated_discrepancies = self.missing_row_candidate(updated_discrepancies, extra_rows_baseline, key_column)
        updated_discrepancies = self.missing_row_baseline(updated_discrepancies, extra_rows_candidate, key_column)

        discrepancies_df = pd.DataFrame(updated_discrepancies)
        discrepancies_df = discrepancies_df.astype(str)
        print(discrepancies_df)

        return discrepancies_df

    def get_generic_message(self, df_baseline, key_column):
        print(
            f"Error: Key identifier '{key_column}' is missing from the dataset. Columns found: {df_baseline.columns.tolist()}"
        )
        raise ValueError(f"Key identifier '{key_column}' not found in dataset.")

    def get_arrays_rules(self, df_merged, discrepancies, key_column, array_rule_key, filters):
        array_rules = self.rules_config["rules"].get(array_rule_key, [])
        # ✅ Dynamically extract sub-columns from rules_config.json
        sub_columns = next(
            (
                rule.get("nested_identifier")
                for rule in array_rules
                if "nested_identifier" in rule
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
            filters
        )
        return discrepancies

    def missing_row_baseline(self, discrepancies, extra_rows_candidate, key_column):
        for _, row in extra_rows_candidate.iterrows():
            discrepancies.append(
                {
                    key_column: row[key_column],
                    "Column Name": "ALL",
                    "Rule Type": "Missing in Baseline",
                    "Classification": "MISSING",
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
                    "Classification": "MISSING",
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

                rule_number = rule.get("rulenumber", "N/A")
                rule_type = rule.get("type", "Unknown")
                rule_description = rule.get("description", "No description available")

                for col in rule.get("columns", []):
                    col_baseline = f"{col}_baseline"
                    col_candidate = f"{col}_candidate"

                    if (
                        col_baseline in df_merged.columns
                        and col_candidate in df_merged.columns
                    ):
                        # Get filter values from the correct nested structure
                        filter_values = {}
                        if filters and "columns" in filters and column_name in filters["columns"]:
                            filter_values = filters["columns"].get(column_name, {}).get(rule_number, {})
                        
                        # Use filter values if available, otherwise use defaults from rule
                        updated_constraint_min = filter_values.get(
                            "min",
                            rule.get("constraints", {}).get("min", float("-inf")),
                        )
                        updated_constraint_max = filter_values.get(
                            "max",
                            rule.get("constraints", {}).get("max", float("inf")),
                        )

                        # Log the filter values being used
                        logging.debug(f"Using filter values for {column_name}.{rule_number}: min={updated_constraint_min}, max={updated_constraint_max}")

                        # Determine data type of the values for comparison
                        sample_baseline = df_merged[col_baseline].dropna().iloc[0] if not df_merged[col_baseline].dropna().empty else None
                        sample_candidate = df_merged[col_candidate].dropna().iloc[0] if not df_merged[col_candidate].dropna().empty else None
                        
                        # Get the non-None sample to check type
                        sample_value = sample_baseline if sample_baseline is not None else sample_candidate
                        
                        if sample_value is not None:
                            # Check if the value can be converted to numeric
                            try:
                                float(str(sample_value))
                                is_numeric = True
                            except (ValueError, TypeError):
                                is_numeric = False
                                
                            # Handle based on data type
                            if is_numeric and "constraints" in rule:
                                # Numeric comparison with constraints
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
                            elif isinstance(sample_value, str):
                                # String comparison with case sensitivity option
                                ignore_case = rule.get("constraints", {}).get("ignorecase", "no").lower() == "yes"
                                
                                if ignore_case:
                                    df_merged["rule_violation"] = df_merged.apply(
                                        lambda row: str(row[col_baseline]).lower() != str(row[col_candidate]).lower() if pd.notna(row[col_baseline]) and pd.notna(row[col_candidate]) else True,
                                        axis=1,
                                    )
                                else:
                                    df_merged["rule_violation"] = df_merged.apply(
                                        lambda row: str(row[col_baseline]) != str(row[col_candidate]) if pd.notna(row[col_baseline]) and pd.notna(row[col_candidate]) else True,
                                        axis=1,
                                    )
                                
                                # Set classification for string comparisons
                                df_merged.loc[
                                    df_merged["rule_violation"], "classification"
                                ] = rule.get("classification", {}).get("other", "VALUE_MISMATCH")
                            else:
                                # Value check comparison (direct equality check)
                                df_merged["rule_violation"] = df_merged.apply(
                                    lambda row: row[col_baseline] != row[col_candidate] if pd.notna(row[col_baseline]) and pd.notna(row[col_candidate]) else True,
                                    axis=1,
                                )
                                df_merged.loc[
                                    df_merged["rule_violation"], "classification"
                                ] = rule.get("classification", {}).get("other", "VALUE_MISMATCH")
                        else:
                            # If no sample value available, do a string comparison as fallback
                            df_merged["rule_violation"] = df_merged.apply(
                                lambda row: str(row[col_baseline]) != str(row[col_candidate]) if pd.notna(row[col_baseline]) and pd.notna(row[col_candidate]) else True,
                                axis=1,
                            )
                            df_merged.loc[
                                df_merged["rule_violation"], "classification"
                            ] = rule.get("classification", {}).get("other", "VALUE_MISMATCH")

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
                    "Classification": row["classification"],
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
        array_rules,
        filters
    ):
        for rule in array_rules:
            rule_number = rule.get("rulenumber", "N/A")
            rule_type = rule.get("type", "Unknown")
            rule_description = rule.get("description", "No description available")
            
            # Get filter values from the correct nested structure
            filter_values = {}
            if filters and "array_fields" in filters and array_rule_key in filters["array_fields"]:
                for rule_filter in filters["array_fields"][array_rule_key]["rules"]:
                    if rule_filter.get("rule_number") == rule_number:
                        filter_values = rule_filter
                        break
            
            # Use filter values if available, otherwise use defaults from rule
            min_threshold = filter_values.get("min", rule.get("constraints", {}).get("min", float("-inf")))
            max_threshold = filter_values.get("max", rule.get("constraints", {}).get("max", float("inf")))
            
            # Log the filter values being used
            logging.debug(f"Using array filter values for {array_rule_key}.{rule_number}: min={min_threshold}, max={max_threshold}")
            
            if "constraints" in rule:
                # Use filter values if available
                min_arrays = float(min_threshold) if min_threshold is not None else rule["constraints"].get("min", min_arrays)
                max_arrays = float(max_threshold) if max_threshold is not None else rule["constraints"].get("max", max_arrays)
            
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
            else:
                array_columns_baseline, array_columns_candidate = [], []

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
                    "Classification": row["classification"],
                    "Rule Number": rule_number,
                    "Description": rule_description,
                    "Baseline Field Value": base_val,
                    "Candidate Field Value": cand_val,
                }
            )
        return discrepancies

    def _extracted_discrepancy_classification(self, rule, df_merged, min_threshold, max_threshold):
        """Classify discrepancies based on rule violation thresholds.
        
        Args:
            rule (dict): The rule containing classification criteria
            df_merged (pd.DataFrame): The merged dataframe with rule violations
            min_threshold (float): Minimum threshold for rule violation
            max_threshold (float): Maximum threshold for rule violation
        """
        classifications = rule.get("classification", {})
        
        # Apply classifications based on thresholds
        df_merged.loc[
            df_merged["rule_violation"] <= min_threshold, 
            "classification"
        ] = classifications.get("min", "BELOW_THRESHOLD")
        
        df_merged.loc[
            df_merged["rule_violation"] >= max_threshold, 
            "classification"
        ] = classifications.get("max", "ABOVE_THRESHOLD")
        
        df_merged.loc[
            (df_merged["rule_violation"] > min_threshold) & 
            (df_merged["rule_violation"] < max_threshold), 
            "classification"
        ] = classifications.get("min_max", "WITHIN_THRESHOLD")

    def get_filter_metadata(self) -> Dict[str, Any]:
        """Get metadata about available filters from rules configuration.
        
        Returns:
            Dictionary containing filter metadata for constraint-based rules only
        """
        metadata = {
            "columns": {},
            "array_fields": {}
        }
        
        try:
            rules_config = self.rules_config
                
            for field_name, field_rules in rules_config.get("rules", {}).items():
                for rule in field_rules:
                    rule_number = rule.get("rulenumber", "")
                    constraints = rule.get("constraints", {})
                    
                    # Only include rules that have constraints
                    if constraints:
                        if field_name not in metadata["columns"]:
                            metadata["columns"][field_name] = []
                            
                        metadata["columns"][field_name].append({
                            "rule_number": rule_number,
                            "rule_type": rule.get("type", ""),
                            "description": rule.get("description", ""),
                            "constraints": constraints,
                            "classification": rule.get("classification", {})
                        })
                        
                    # Handle array fields with constraints
                    if rule.get("format_type") == "Array" and constraints:
                        if field_name not in metadata["array_fields"]:
                            metadata["array_fields"][field_name] = {
                                "nested_identifier": rule.get("nested_identifier", []),
                                "rules": []
                            }
                        metadata["array_fields"][field_name]["rules"].append({
                            "rule_number": rule_number,
                            "constraints": constraints,
                            "classification": rule.get("classification", {})
                        })
                        
            return metadata
            
        except Exception as e:
            logging.error(f"Error getting filter metadata: {str(e)}")
            return metadata

    def process_filters(self, selected_filters=None) -> Dict[str, Any]:
        """Process and validate filter settings.
        
        Args:
            selected_filters: Dictionary of selected filter values
            
        Returns:
            Dictionary of processed filters
        """
        if not selected_filters:
            return {}
        
        processed_filters = {
            "columns": {},
            "array_fields": {}
        }
        
        try:
            # Process column filters
            for column_name, rules in selected_filters.get("columns", {}).items():
                processed_filters["columns"][column_name] = {}
                for rule_number, rule_data in rules.items():
                    processed_filters["columns"][column_name][rule_number] = {
                        "min": float(rule_data.get("min", 0)) if rule_data.get("min") is not None else float("-inf"),
                        "max": float(rule_data.get("max", float('inf'))) if rule_data.get("max") is not None else float("inf"),
                        "format_type": rule_data.get("format_type", ""),
                        "rule_type": rule_data.get("rule_type", "")
                    }
            
            # Process array field filters
            for field_name, field_data in selected_filters.get("array_fields", {}).items():
                processed_filters["array_fields"][field_name] = {
                    "nested_identifier": field_data.get("nested_identifier", []),
                    "rules": []
                }
                
                for rule in field_data.get("rules", []):
                    processed_rule = {
                        "rule_number": rule.get("rule_number", ""),
                        "rule_type": rule.get("rule_type", "")
                    }
                    
                    # Process min/max values
                    if "min" in rule:
                        try:
                            processed_rule["min"] = float(rule["min"])
                        except (ValueError, TypeError):
                            processed_rule["min"] = float("-inf")
                    else:
                        processed_rule["min"] = float("-inf")
                        
                    if "max" in rule:
                        try:
                            processed_rule["max"] = float(rule["max"])
                        except (ValueError, TypeError):
                            processed_rule["max"] = float("inf")
                    else:
                        processed_rule["max"] = float("inf")
                    
                    # Add any other constraint keys
                    for key, value in rule.items():
                        if key not in ["rule_number", "rule_type", "min", "max"]:
                            processed_rule[key] = value
                    
                    processed_filters["array_fields"][field_name]["rules"].append(processed_rule)
            
            # Log the processed filters
            logging.debug(f"Processed filters: {processed_filters}")
                    
            return processed_filters
            
        except Exception as e:
            logging.error(f"Error processing filters: {str(e)}")
            return {}
