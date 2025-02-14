import pandas as pd
import json
import re
from typing import List, Callable, Dict

class ValidationRule:
    """Defines a validation rule that applies to both files."""
    def __init__(self, columns: List[str], validation_func: Callable, description: str):
        self.columns = columns
        self.validation_func = validation_func
        self.description = description

    def validate(self, df: pd.DataFrame) -> Dict:
        """Run validation and return results."""
        results = {"passed": True, "details": [], "failed_rows": 0}

        try:
            validation_result = self.validation_func(df[self.columns])

            if isinstance(validation_result, bool):
                results["passed"] = validation_result
                results["failed_rows"] = 0 if validation_result else len(df)
            else:
                failed_mask = ~validation_result
                results["passed"] = not failed_mask.any()
                results["failed_rows"] = failed_mask.sum()
                if not results["passed"]:
                    results["details"] = df[failed_mask].index.tolist()
        except Exception as e:
            results["passed"] = False
            results["details"] = [str(e)]
            results["failed_rows"] = len(df)

        return results

class ValidationRuleLoader:
    """Loads validation rules from JSON."""
    @staticmethod
    def load_rules(config_path: str) -> List[ValidationRule]:
        with open(config_path, "r") as file:
            config = json.load(file)

        rules = []
        for rule in config["rules"]:
            rule_type = rule["type"]
            columns = rule["columns"]

            if rule_type == "not_null":
                func = lambda df: df.notna().all(axis=1)
            elif rule_type == "unique":
                func = lambda df: df.nunique() == len(df)
            elif rule_type == "regex":
                pattern = re.compile(rule["pattern"])
                func = lambda df: df.astype(str).str.match(pattern)
            elif rule_type == "value_range":
                min_val, max_val = rule["min"], rule["max"]
                func = lambda df: (df >= min_val) & (df <= max_val)
            else:
                continue

            rules.append(ValidationRule(columns, func, rule["description"]))

        return rules
