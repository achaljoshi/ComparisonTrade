import argparse
import os
from utils.data_processor import DataProcessor

def get_absolute_path(path):
    """Convert relative paths to absolute paths."""
    return os.path.abspath(path)

def main():
    parser = argparse.ArgumentParser(description="Discrepancy Detection CLI Tool")

    parser.add_argument("--baseline", required=True, help="Path to the baseline file")
    parser.add_argument("--candidate", required=True, help="Path to the candidate file")
    parser.add_argument("--directory_config", required=True, help="Path to directory_config.json")
    parser.add_argument("--job_response", required=True, help="Path to job_creation_response.json")
    parser.add_argument("--rules_config", required=True, help="Path to rules_config.json")
    parser.add_argument("--output", default="discrepancy_report.csv", help="Output file path")
    parser.add_argument("--file_type", choices=["Excel", "CSV"], default="Excel", help="File type")
    parser.add_argument("--format", choices=["csv", "json", "excel"], default="csv", help="Output format")

    args = parser.parse_args()

    # Convert paths to absolute paths
    baseline_path = get_absolute_path(args.baseline)
    candidate_path = get_absolute_path(args.candidate)
    directory_config_path = get_absolute_path(args.directory_config)
    job_response_path = get_absolute_path(args.job_response)
    rules_config_path = get_absolute_path(args.rules_config)
    output_path = get_absolute_path(args.output)

    # Initialize tool
    tool = DataProcessor(directory_config_path, job_response_path, rules_config_path)

    # Run comparison
    results = tool.run_comparison(baseline_path, candidate_path, args.file_type)

    # Save results
    tool.save_results(results, output_path, args.format)

if __name__ == "__main__":
    main()
