from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Optional
import pandas as pd
import dask.dataframe as dd
import numpy as np

@dataclass
class DiscrepancyRecord:
    identifier: str
    column_name: str
    rule_type: str
    category: str
    rule_number: str
    description: str
    baseline_value: str
    candidate_value: str
    difference: str
    timestamp: str = datetime.now().isoformat()

@dataclass
class ComparisonResult:
    discrepancies: pd.DataFrame
    metadata: Dict
    timestamp: datetime = datetime.now()

class DataProcessor:
    def __init__(self, directory_config: Dict, job_response: Dict, rules_config: Dict):
        self.directory_config = directory_config
        self.job_response = job_response
        self.rules_config = rules_config
        self.key_column = self.rules_config["identifier"]

    def format_discrepancy(self, **kwargs) -> DiscrepancyRecord:
        return DiscrepancyRecord(
            identifier=kwargs["identifier"],
            column_name=kwargs["column_name"],
            rule_type=kwargs["rule_type"],
            category=kwargs["category"].upper(),
            rule_number=kwargs["rule_number"],
            description=kwargs["description"],
            baseline_value=str(kwargs["baseline_value"]),
            candidate_value=str(kwargs["candidate_value"]),
            difference=str(kwargs.get("difference", "N/A")),
            timestamp=datetime.now().isoformat()
        )

    def compare_files(self, df_baseline: pd.DataFrame, df_candidate: pd.DataFrame, 
                     file_type: str, filters: Optional[Dict] = None) -> ComparisonResult:
        ddf_baseline = dd.from_pandas(df_baseline, npartitions=100)
        ddf_candidate = dd.from_pandas(df_candidate, npartitions=100)
        
        merged_df = self._merge_dataframes(ddf_baseline, ddf_candidate)
        discrepancies = self._process_discrepancies(merged_df, filters)
        
        metadata = self._create_metadata(df_baseline, df_candidate, file_type)
        return ComparisonResult(discrepancies=discrepancies, metadata=metadata)

    def _merge_dataframes(self, df_baseline: dd.DataFrame, df_candidate: dd.DataFrame) -> dd.DataFrame:
        return df_baseline.merge(
            df_candidate,
            on=self.key_column,
            how='outer',
            suffixes=('_baseline', '_candidate'),
            indicator=True
        )

    def _process_discrepancies(self, merged_df: dd.DataFrame, filters: Optional[Dict]) -> pd.DataFrame:
        discrepancies = []
        
        # Process missing records
        discrepancies.extend(self._process_missing_records(merged_df))
        
        # Process value discrepancies
        matching_records = merged_df[merged_df._merge == 'both']
        discrepancies.extend(self._process_value_discrepancies(matching_records, filters))
        
        return pd.DataFrame([d.__dict__ for d in discrepancies])

    def _process_missing_records(self, merged_df: dd.DataFrame) -> list:
        missing_records = []
        
        missing_baseline = merged_df[merged_df._merge == 'right_only'].compute()
        missing_candidate = merged_df[merged_df._merge == 'left_only'].compute()
        
        for _, row in missing_baseline.iterrows():
            missing_records.append(self.format_discrepancy(
                identifier=row[self.key_column],
                column_name="ALL",
                rule_type="Missing in Baseline",
                category="MISSING",
                rule_number="MISSING_ROW",
                description="Record exists in candidate but missing in baseline",
                baseline_value="MISSING",
                candidate_value=str(row.filter(like='_candidate').to_dict())
            ))
            
        for _, row in missing_candidate.iterrows():
            missing_records.append(self.format_discrepancy(
                identifier=row[self.key_column],
                column_name="ALL",
                rule_type="Missing in Candidate",
                category="MISSING",
                rule_number="MISSING_ROW",
                description="Record exists in baseline but missing in candidate",
                baseline_value=str(row.filter(like='_baseline').to_dict()),
                candidate_value="MISSING"
            ))
            
        return missing_records

    def _process_value_discrepancies(self, matching_records: dd.DataFrame, filters: Optional[Dict]) -> list:
        discrepancies = []
        
        for rule in self.rules_config["rules"]:
            rule_discrepancies = self._apply_rule(matching_records, rule, filters)
            discrepancies.extend(rule_discrepancies)
            
        return discrepancies

    def _apply_rule(self, df: dd.DataFrame, rule: Dict, filters: Optional[Dict]) -> list:
        if filters and rule["Rule Number"] in filters:
            rule = {**rule, **filters[rule["Rule Number"]]}
            
        discrepancies = []
        for column in rule["columns"]:
            col_baseline = f"{column}_baseline"
            col_candidate = f"{column}_candidate"
            
            violations = self._check_violations(df, col_baseline, col_candidate, rule)
            discrepancies.extend(violations)
            
        return discrepancies

    def _check_violations(self, df: dd.DataFrame, col_baseline: str, col_candidate: str, rule: Dict) -> list:
        violations = []
        computed_df = df.compute()
        
        if "acceptable" in rule or "warning" in rule or "fatal" in rule:
            violations.extend(self._check_threshold_violations(computed_df, col_baseline, col_candidate, rule))
        elif "threshold" in rule:
            violations.extend(self._check_simple_threshold(computed_df, col_baseline, col_candidate, rule))
        elif "days" in rule:
            violations.extend(self._check_date_violations(computed_df, col_baseline, col_candidate, rule))
        else:
            violations.extend(self._check_string_violations(computed_df, col_baseline, col_candidate, rule))
            
        return violations

    def _create_metadata(self, df_baseline: pd.DataFrame, df_candidate: pd.DataFrame, file_type: str) -> Dict:
        # Convert Dask DataFrame to Pandas if needed
        if hasattr(df_baseline, 'compute'):
            df_baseline = df_baseline.compute()
        if hasattr(df_candidate, 'compute'):
            df_candidate = df_candidate.compute()

        return {
            "file_type": file_type,
            "baseline_records": len(df_baseline),
            "candidate_records": len(df_candidate),
            "baseline_env": self.job_response["baseline"]["env"],
            "candidate_env": self.job_response["candidate"]["env"],
            "comparison_timestamp": datetime.now().isoformat()
        }