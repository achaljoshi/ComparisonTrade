from typing import Dict, List, Optional, Tuple
import pandas as pd
import asyncio
from pathlib import Path
import concurrent.futures
from .data_processor import DataProcessor

class BatchProcessor:
    def __init__(self):
        self.results = {}
        self.errors = {}
        self.progress = 0
        self.total_pairs = 0
        
    async def process_file_pair(
        self, 
        file1: Path, 
        file2: Path, 
        pair_id: str,
        sheet1: Optional[str] = None,
        sheet2: Optional[str] = None
    ) -> Dict:
        """Process a single pair of files asynchronously"""
        try:
            processor = DataProcessor()
            df1, df2 = processor.process_files(file1, file2, sheet1, sheet2)
            discrepancies = processor.find_discrepancies()
            validation_results = processor.validate_data()
            
            self.results[pair_id] = {
                'discrepancies': discrepancies,
                'validation': validation_results,
                'type_analysis': processor.get_type_analysis()
            }
            
            return {
                'status': 'success',
                'pair_id': pair_id,
                'message': 'Successfully processed file pair'
            }
            
        except Exception as e:
            self.errors[pair_id] = str(e)
            return {
                'status': 'error',
                'pair_id': pair_id,
                'message': str(e)
            }
        finally:
            self.progress += 1
    
    def get_progress(self) -> Tuple[int, int]:
        """Get current progress"""
        return self.progress, self.total_pairs
    
    def get_progress_percentage(self) -> float:
        """Get progress as percentage"""
        if self.total_pairs == 0:
            return 0
        return (self.progress / self.total_pairs) * 100
    
    async def process_batch(
        self,
        file_pairs: List[Dict[str, str]],
        max_concurrent: int = 3
    ) -> Dict:
        """Process multiple file pairs concurrently"""
        self.progress = 0
        self.total_pairs = len(file_pairs)
        self.results = {}
        self.errors = {}
        
        # Create tasks for each file pair
        tasks = []
        for pair in file_pairs:
            task = self.process_file_pair(
                Path(pair['file1']),
                Path(pair['file2']),
                pair.get('pair_id', f"pair_{len(tasks)}"),
                pair.get('sheet1'),
                pair.get('sheet2')
            )
            tasks.append(task)
        
        # Process files in batches
        results = []
        for i in range(0, len(tasks), max_concurrent):
            batch = tasks[i:i + max_concurrent]
            batch_results = await asyncio.gather(*batch)
            results.extend(batch_results)
        
        return {
            'total_pairs': self.total_pairs,
            'successful': len(self.results),
            'failed': len(self.errors),
            'results': self.results,
            'errors': self.errors
        }
    
    def get_summary_report(self) -> pd.DataFrame:
        """Generate summary report for all processed pairs"""
        summary_data = []
        
        for pair_id, result in self.results.items():
            discrepancies = result['discrepancies']
            validation = result['validation']
            
            summary_data.append({
                'pair_id': pair_id,
                'total_mismatches': discrepancies['summary']['value_mismatches'],
                'missing_in_file1': discrepancies['summary']['missing_in_df1'],
                'missing_in_file2': discrepancies['summary']['missing_in_df2'],
                'validation_passed': validation['file1']['summary']['passed_rules'],
                'validation_failed': validation['file1']['summary']['failed_rules'],
                'total_violations': validation['file1']['summary']['total_violations']
            })
        
        return pd.DataFrame(summary_data) 