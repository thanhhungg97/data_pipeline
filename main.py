"""Main ETL Pipeline - Multi-Layer Architecture.

Usage:
    python main.py                    # Run full pipeline (all layers, all sources)
    python main.py --source shopee    # Run single source
    python main.py --layer bronze     # Run only bronze layer
    python main.py --layer silver     # Run only silver layer
    python main.py --layer gold       # Run only gold layer
    python main.py --legacy           # Run old single-layer pipeline
"""

import argparse


def run_multi_layer_pipeline(sources=None, layers=None):
    """Run the multi-layer ETL pipeline."""
    from src.etl import run_pipeline

    return run_pipeline(sources=sources, layers=layers)


def run_legacy_pipeline():
    """Run the old single-layer pipeline (for backward compatibility)."""
    from src.extract import extract_all_sources, load_config
    from src.load import save_all
    from src.transform import run_transforms

    print("=" * 50)
    print("LEGACY ETL PIPELINE (Single-Layer)")
    print("=" * 50)

    config = load_config()
    output_dir = config["paths"]["output_dir"]
    output_format = config["output"]["format"]

    # EXTRACT
    print("\n[1/3] EXTRACT")
    print("-" * 30)
    dataframes = extract_all_sources(config)

    if not dataframes:
        print("No data files found.")
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


def main():
    parser = argparse.ArgumentParser(description="Multi-Layer ETL Pipeline")
    parser.add_argument(
        "--source",
        "-s",
        type=str,
        help="Run specific source (e.g., shopee, website_suppersport)",
    )
    parser.add_argument(
        "--layer",
        "-l",
        type=str,
        choices=["bronze", "silver", "gold", "all"],
        default="all",
        help="Which layer(s) to run",
    )
    parser.add_argument(
        "--legacy",
        action="store_true",
        help="Run legacy single-layer pipeline",
    )

    args = parser.parse_args()

    if args.legacy:
        run_legacy_pipeline()
        return

    # Determine layers
    if args.layer == "all":
        layers = ["bronze", "silver", "gold"]
    else:
        layers = [args.layer]

    # Determine sources
    sources = [args.source] if args.source else None

    # Run multi-layer pipeline
    run_multi_layer_pipeline(sources=sources, layers=layers)


if __name__ == "__main__":
    main()
