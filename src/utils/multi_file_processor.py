from typing import Dict, List, Optional, Tuple
import pandas as pd
import numpy as np
from pathlib import Path
from .data_processor import DataProcessor
from itertools import combinations

class MultiFileProcessor:
    def __init__(self):
        self.files = {}  # Dict to store DataFrames
        self.common_columns = None
        self.processor = DataProcessor()
        self.comparison_results = {}
        
    def add_file(self, file_id: str, file_path: Path, sheet_name: Optional[str] = None) -> None:
        """Add a file to the comparison set"""
        try:
            df = self.processor.read_excel(file_path, sheet_name)
            self.files[file_id] = {
                'df': df,
                'path': file_path,
                'sheet': sheet_name
            }
            self._update_common_columns()
        except Exception as e:
            raise Exception(f"Error adding file {file_id}: {str(e)}")
    
    def _update_common_columns(self) -> None:
        """Update common columns across all files"""
        if not self.files:
            self.common_columns = None
            return
            
        column_sets = [set(file_info['df'].columns) for file_info in self.files.values()]
        self.common_columns = list(set.intersection(*column_sets))
    
    def compare_all(self, key_columns: Optional[List[str]] = None) -> Dict:
        """Compare all files with each other"""
        if len(self.files) < 2:
            raise ValueError("Need at least 2 files for comparison")
            
        # Generate all possible pairs
        file_pairs = list(combinations(self.files.keys(), 2))
        
        # Compare each pair
        for file1_id, file2_id in file_pairs:
            comparison_key = f"{file1_id}_vs_{file2_id}"
            
            # Create temporary processor for this comparison
            temp_processor = DataProcessor()
            temp_processor.df1 = self.files[file1_id]['df']
            temp_processor.df2 = self.files[file2_id]['df']
            temp_processor.common_columns = self.common_columns
            
            if key_columns:
                temp_processor.set_key_columns(key_columns)
            
            # Find discrepancies
            self.comparison_results[comparison_key] = {
                'discrepancies': temp_processor.find_discrepancies(),
                'validation': temp_processor.validate_data(),
                'type_analysis': temp_processor.get_type_analysis()
            }
        
        return self.get_summary()
    
    def get_summary(self) -> Dict:
        """Get summary of all comparisons"""
        if not self.comparison_results:
            return {}
            
        summary = {
            'total_files': len(self.files),
            'total_comparisons': len(self.comparison_results),
            'common_columns': len(self.common_columns),
            'comparisons': {}
        }
        
        # Summarize each comparison
        for comp_key, results in self.comparison_results.items():
            file1_id, file2_id = comp_key.split('_vs_')
            discrepancies = results['discrepancies']
            
            summary['comparisons'][comp_key] = {
                'file1': {
                    'id': file1_id,
                    'path': str(self.files[file1_id]['path']),
                    'total_rows': discrepancies['summary']['total_rows_df1']
                },
                'file2': {
                    'id': file2_id,
                    'path': str(self.files[file2_id]['path']),
                    'total_rows': discrepancies['summary']['total_rows_df2']
                },
                'mismatches': discrepancies['summary']['value_mismatches'],
                'missing_rows': {
                    'in_file1': discrepancies['summary']['missing_in_df1'],
                    'in_file2': discrepancies['summary']['missing_in_df2']
                }
            }
        
        # Calculate overall statistics
        total_mismatches = sum(comp['mismatches'] 
                             for comp in summary['comparisons'].values())
        total_missing = sum(comp['missing_rows']['in_file1'] + comp['missing_rows']['in_file2']
                          for comp in summary['comparisons'].values())
        
        summary['overall_stats'] = {
            'total_mismatches': total_mismatches,
            'total_missing_rows': total_missing,
            'average_mismatches_per_comparison': total_mismatches / len(self.comparison_results)
        }
        
        return summary
    
    def get_comparison_matrix(self) -> pd.DataFrame:
        """Generate comparison matrix showing differences between all files"""
        file_ids = list(self.files.keys())
        matrix = pd.DataFrame(0, index=file_ids, columns=file_ids)
        
        for comp_key, results in self.comparison_results.items():
            file1_id, file2_id = comp_key.split('_vs_')
            total_diff = (results['discrepancies']['summary']['value_mismatches'] +
                        results['discrepancies']['summary']['missing_in_df1'] +
                        results['discrepancies']['summary']['missing_in_df2'])
            
            matrix.loc[file1_id, file2_id] = total_diff
            matrix.loc[file2_id, file1_id] = total_diff
        
        return matrix 