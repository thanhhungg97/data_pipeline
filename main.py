"""Main ETL Pipeline Orchestrator."""
import yaml
from pathlib import Path

from src.extract import read_all_excel_files
from src.transform import run_transforms
from src.load import save_all


def load_config(config_path: str = "config.yaml") -> dict:
    """Load configuration from YAML file."""
    with open(config_path) as f:
        return yaml.safe_load(f)


def run_pipeline():
    """Run the full ETL pipeline."""
    print("=" * 50)
    print("STARTING ETL PIPELINE")
    print("=" * 50)
    
    # Load config
    config = load_config()
    input_dir = config["paths"]["input_dir"]
    output_dir = config["paths"]["output_dir"]
    output_format = config["output"]["format"]
    
    # EXTRACT
    print("\n[1/3] EXTRACT")
    print("-" * 30)
    dataframes = read_all_excel_files(input_dir)
    
    if not dataframes:
        print(f"No Excel files found in {input_dir}")
        print("Add your .xlsx or .xls files to that directory and run again.")
        return
    
    # TRANSFORM
    print("\n[2/3] TRANSFORM")
    print("-" * 30)
    results = run_transforms(dataframes)
    
    # LOAD
    print("\n[3/3] LOAD")
    print("-" * 30)
    save_all(results, output_dir, format=output_format, partition_by_date=True)
    
    print("\n" + "=" * 50)
    print("PIPELINE COMPLETE")
    print("=" * 50)


if __name__ == "__main__":
    run_pipeline()

