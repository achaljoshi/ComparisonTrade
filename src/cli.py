import click
import json
import pandas as pd
from pathlib import Path
from typing import Dict, Optional
from datetime import datetime
from utils.data_processor import DataProcessor, ComparisonResult

@click.group()
def cli():
    """Discrepancy Analysis CLI Tool"""
    pass

@cli.command()
@click.option('--baseline', type=click.Path(exists=True), required=True, help='Path to baseline file')
@click.option('--candidate', type=click.Path(exists=True), required=True, help='Path to candidate file')
@click.option('--directory-config', type=click.Path(exists=True), required=True, help='Path to directory config')
@click.option('--job-response', type=click.Path(exists=True), required=True, help='Path to job response config')
@click.option('--rules-config', type=click.Path(exists=True), required=True, help='Path to rules config')
@click.option('--file-type', type=click.Choice(['Excel', 'Text', 'DD']), required=True, help='Type of files to compare')
@click.option('--output', type=click.Path(), required=True, help='Path to save comparison results')
@click.option('--filters', type=click.Path(exists=True), help='Path to filters JSON file')
def compare(baseline: str, candidate: str, directory_config: str, 
           job_response: str, rules_config: str, file_type: str, 
           output: str, filters: Optional[str] = None):
    """Run comparison between baseline and candidate files"""
    try:
        processor = DataProcessor(
            json.load(open(directory_config)),
            json.load(open(job_response)),
            json.load(open(rules_config))
        )
        
        df_baseline = read_input_file(Path(baseline), file_type)
        df_candidate = read_input_file(Path(candidate), file_type)
        
        results = processor.compare_files(
            df_baseline,
            df_candidate,
            file_type,
            json.load(open(filters)) if filters else None
        )
        
        save_results(results, Path(output))
        display_summary(results)

    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)
        raise click.Abort()

def load_configs(directory_config: str, job_response: str, rules_config: str) -> Dict:
    """Load configuration files"""
    return {
        "directory_config": json.load(open(directory_config)),
        "job_response": json.load(open(job_response)),
        "rules_config": json.load(open(rules_config))
    }

def load_filters(filters_path: str) -> Dict:
    """Load filter rules from JSON file"""
    return json.load(open(filters_path))

def read_input_file(file_path: Path, file_type: str) -> pd.DataFrame:
    """Read input file based on file type"""
    if file_type == "Excel":
        return pd.read_excel(file_path)
    return pd.read_csv(file_path)

def save_results(results: ComparisonResult, output_path: Path, job_response: Dict) -> None:
    """Save comparison results in specified format"""
    export_format = job_response.get("report", {}).get("format", "CSV").upper()
    
    if export_format == "CSV":
        results.discrepancies.to_csv(output_path, index=False)
    elif export_format == "EXCEL":
        results.discrepancies.to_excel(output_path, index=False)
    elif export_format == "JSON":
        results.discrepancies.to_json(output_path, orient="records", indent=4)
    else:
        results.discrepancies.to_csv(output_path, index=False)

def display_summary(results: ComparisonResult) -> None:
    """Display comparison summary"""
    click.echo("\nComparison Summary:")
    click.echo(f"Total Discrepancies: {len(results.discrepancies)}")
    
    category_counts = results.discrepancies["Category"].value_counts()
    click.echo("\nDiscrepancies by Category:")
    for category, count in category_counts.items():
        click.echo(f"{category}: {count}")
    
    missing_baseline = (results.discrepancies["Rule Type"] == "Missing in Baseline").sum()
    missing_candidate = (results.discrepancies["Rule Type"] == "Missing in Candidate").sum()
    click.echo(f"\nMissing in Baseline: {missing_baseline}")
    click.echo(f"Missing in Candidate: {missing_candidate}")

@cli.command()
def version():
    """Display version information"""
    click.echo("Discrepancy Analysis CLI v1.0.0")

if __name__ == "__main__":
    cli()
