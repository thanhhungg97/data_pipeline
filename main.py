"""Main ETL Pipeline Orchestrator - Multi-Source Support."""
import yaml
from pathlib import Path

from src.extract import extract_all_sources, load_config
from src.transform import run_transforms
from src.load import save_all


def run_pipeline():
    """Run the full ETL pipeline for all configured sources."""
    print("=" * 50)
    print("STARTING ETL PIPELINE (Multi-Source)")
    print("=" * 50)
    
    # Load config
    config = load_config()
    output_dir = config["paths"]["output_dir"]
    output_format = config["output"]["format"]
    
    # EXTRACT - from all configured sources
    print("\n[1/3] EXTRACT")
    print("-" * 30)
    dataframes = extract_all_sources(config)
    
    if not dataframes:
        print("No data files found. Check your config.yaml sources configuration.")
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
    
    # Summary
    if "all_sources" in results:
        df = results["all_sources"]
        sources = df["Source"].unique().to_list()
        print(f"\n📊 Summary:")
        print(f"   Sources: {sources}")
        print(f"   Total rows: {len(df):,}")


if __name__ == "__main__":
    run_pipeline()
