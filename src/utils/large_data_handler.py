import dask.dataframe as dd
import os
from .validator import ValidationRuleLoader

class LargeDataHandler:
    def __init__(self, validation_config_path):
        self.validation_rules = ValidationRuleLoader.load_rules(validation_config_path)

    def validate_large_file(self, uploaded_file):
        """Validate large files with Dask while handling cross-platform file uploads."""
        file_extension = uploaded_file.name.split(".")[-1].lower()
        
        # Ensure Dask reads files correctly across platforms
        if file_extension == "csv":
            df = dd.read_csv(uploaded_file)
        elif file_extension in ["xls", "xlsx"]:
            df = dd.read_parquet(uploaded_file)
        else:
            raise ValueError("Unsupported file format. Please upload CSV or Excel files.")

        validation_results = {}
        for rule in self.validation_rules:
            validation_results[rule.description] = rule.validate(df.compute())  # Convert Dask DF to Pandas

        return validation_results
