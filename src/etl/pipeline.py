"""Pipeline orchestrator for multi-layer ETL."""

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import polars as pl
import yaml

from .base import ETLConfig, save_partitioned
from .bronze import BronzeETL, ShopeeBronzeETL, WebsiteBronzeETL
from .gold import GoldETL
from .silver import ShopeeSilverETL, SilverETL, WebsiteSilverETL

# Registry of source-specific ETL classes
BRONZE_REGISTRY = {
    "shopee": ShopeeBronzeETL,
    "website_suppersport": WebsiteBronzeETL,
    "website_columbia": WebsiteBronzeETL,
    "website_underamour": WebsiteBronzeETL,
    # Add more sources here
}

SILVER_REGISTRY = {
    "shopee": ShopeeSilverETL,
    "website_suppersport": WebsiteSilverETL,
    "website_columbia": WebsiteSilverETL,
    "website_underamour": WebsiteSilverETL,
    # Add more sources here
}


def get_bronze_etl(source: str, config: ETLConfig) -> BronzeETL:
    """Get the appropriate Bronze ETL for a source."""
    etl_class = BRONZE_REGISTRY.get(source.lower(), BronzeETL)
    return etl_class(config)


def get_silver_etl(source: str, config: ETLConfig) -> SilverETL:
    """Get the appropriate Silver ETL for a source."""
    etl_class = SILVER_REGISTRY.get(source.lower(), SilverETL)
    return etl_class(config)


def discover_sources(input_dir: str = "data/raw") -> list[str]:
    """Discover available sources from directory structure."""
    input_path = Path(input_dir)
    sources = []

    if not input_path.exists():
        return sources

    for item in sorted(input_path.iterdir()):
        if item.is_dir() and not item.name.startswith("."):
            sources.append(item.name)

    return sources


def run_source_pipeline(
    source: str,
    config_path: str = "config.yaml",
    layers: list[str] = None,
) -> dict:
    """Run ETL pipeline for a single source.

    Args:
        source: Source name (e.g., "shopee", "website_suppersport")
        config_path: Path to config file
        layers: Which layers to run ["bronze", "silver"] or None for all

    Returns:
        Dict with results from each layer
    """
    if layers is None:
        layers = ["bronze", "silver"]

    config = ETLConfig.from_yaml(config_path, source)
    results = {"source": source}

    print(f"\n{'#' * 60}")
    print(f"# SOURCE: {source.upper()}")
    print(f"{'#' * 60}")

    # Bronze Layer
    if "bronze" in layers:
        print("\n[BRONZE] Extracting raw data...")
        bronze_etl = get_bronze_etl(source, config)
        bronze_df = bronze_etl.run()
        results["bronze_rows"] = len(bronze_df)

    # Silver Layer
    if "silver" in layers:
        print("\n[SILVER] Transforming & normalizing...")
        silver_etl = get_silver_etl(source, config)
        silver_df = silver_etl.run()
        results["silver_rows"] = len(silver_df)

    return results


def run_pipeline(
    config_path: str = "config.yaml",
    sources: list[str] = None,
    layers: list[str] = None,
) -> dict:
    """Run the full multi-layer ETL pipeline.

    Args:
        config_path: Path to config file
        sources: List of sources to process, or None for auto-discover
        layers: Which layers to run ["bronze", "silver", "gold"] or None for all

    Returns:
        Dict with results
    """
    if layers is None:
        layers = ["bronze", "silver", "gold"]

    # Discover sources if not provided
    if sources is None:
        sources = discover_sources()

    if not sources:
        print("❌ No sources found!")
        return {"error": "No sources found"}

    print("=" * 60)
    print("MULTI-LAYER ETL PIPELINE")
    print("=" * 60)
    print(f"\nSources: {sources}")
    print(f"Layers: {layers}")

    results = {"sources": {}}

    # Run Bronze & Silver for each source
    for source in sources:
        source_layers = [layer for layer in layers if layer in ["bronze", "silver"]]
        if source_layers:
            result = run_source_pipeline(source, config_path, source_layers)
            results["sources"][source] = result

    # Run Gold layer (combine all)
    if "gold" in layers:
        print(f"\n{'#' * 60}")
        print("# GOLD LAYER: Combining all sources")
        print(f"{'#' * 60}")

        config = ETLConfig.from_yaml(config_path)
        gold_etl = GoldETL(config, silver_sources=sources)
        gold_df = gold_etl.run()
        results["gold_rows"] = len(gold_df)

    # Summary
    print("\n" + "=" * 60)
    print("PIPELINE COMPLETE")
    print("=" * 60)
    print("\n📊 Summary:")
    for source, data in results.get("sources", {}).items():
        bronze = data.get("bronze_rows", "N/A")
        silver = data.get("silver_rows", "N/A")
        print(
            f"   {source}: Bronze={bronze:,}, Silver={silver:,}"
            if isinstance(bronze, int)
            else f"   {source}: Bronze={bronze}, Silver={silver}"
        )

    if "gold_rows" in results:
        print(f"   Gold (combined): {results['gold_rows']:,} rows")

    return results


# ============================================================
# SIMPLE MODE: For GUI and single-folder processing
# ============================================================


@dataclass
class SimpleETLConfig:
    """Simplified config for single-folder ETL."""

    input_dir: Path
    output_dir: Path
    status_mapping: dict
    schema: dict


def _parse_date_column(df: pl.DataFrame) -> pl.DataFrame:
    """Parse Date column with multiple format attempts."""
    if "Date" not in df.columns:
        return df

    # If already datetime/date, return as-is
    if df["Date"].dtype in (pl.Datetime, pl.Date):
        return df

    # Try different date formats for string columns
    date_formats = [
        "%m-%d-%y",  # 01-13-24
        "%m/%d/%y",  # 01/13/24
        "%Y-%m-%d",  # 2024-01-13
        "%d-%m-%Y",  # 13-01-2024
        "%m-%d-%Y",  # 01-13-2024
    ]

    for fmt in date_formats:
        try:
            parsed = df.with_columns(
                pl.col("Date").str.strptime(pl.Date, fmt, strict=False).alias("Date")
            )
            # Check if parsing worked (not all nulls)
            if parsed["Date"].null_count() < len(parsed):
                return parsed
        except Exception:
            continue

    # Fallback: try automatic casting with strict=False
    try:
        return df.with_columns(pl.col("Date").cast(pl.Date, strict=False).alias("Date"))
    except Exception:
        return df


def load_config(config_path: str = "config.yaml") -> dict:
    """Load config from YAML file."""
    try:
        with open(config_path, encoding="utf-8") as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        # Return defaults if no config file
        return {
            "status_mapping": {
                "Delivered": "Delivered",
                "Completed": "Delivered",
                "Done": "Delivered",
                "Cancel by cust.": "Cancelled",
                "Cancelled": "Cancelled",
                "Canceled": "Cancelled",
                "Returned": "Returned",
                "Return": "Returned",
                "Refunded": "Returned",
                "Failed delivery": "Failed",
                "Failed": "Failed",
                "Delivery Failed": "Failed",
            }
        }


def run_simple_etl(
    input_dir: str,
    output_dir: str,
    config_path: str = "config.yaml",
    progress_callback: Callable[[str], None] = None,
) -> dict:
    """Run simplified ETL pipeline for a single folder.

    This is the shared logic used by both CLI and GUI.

    Args:
        input_dir: Directory containing Excel files
        output_dir: Directory to write processed parquet files
        config_path: Path to config.yaml
        progress_callback: Optional callback for progress messages

    Returns:
        Dict with results (row counts, output path, etc.)
    """

    def log(msg: str):
        if progress_callback:
            progress_callback(msg)
        else:
            print(msg)

    input_path = Path(input_dir)
    output_path = Path(output_dir)
    config = load_config(config_path)
    status_mapping = config.get("status_mapping", {})

    # ========== EXTRACT ==========
    log("📂 Reading Excel files...")
    excel_files = list(input_path.glob("*.xlsx")) + list(input_path.glob("*.xls"))

    if not excel_files:
        raise FileNotFoundError(f"No Excel files found in {input_dir}")

    dataframes = []
    file_errors = []
    files_loaded = 0

    for file in excel_files:
        log(f"  📄 {file.name}...")
        try:
            df = pl.read_excel(file)

            # Validate required columns
            if df.is_empty():
                file_errors.append({"file": file.name, "error": "File is empty"})
                log("     ⚠️ Empty file, skipped")
                continue

            # Parse date column with multiple format support
            if "Date" in df.columns:
                df = _parse_date_column(df)
                # Check for parsing failures
                if df["Date"].null_count() > len(df) * 0.5:
                    file_errors.append(
                        {
                            "file": file.name,
                            "error": f"Date parsing failed for {df['Date'].null_count()}/{len(df)} rows",
                            "warning": True,
                        }
                    )
                    log(f"     ⚠️ {df['Date'].null_count()} rows with invalid dates")

            dataframes.append(df)
            files_loaded += 1
            log(f"     ✓ {len(df):,} rows")

        except Exception as e:
            error_msg = str(e)
            # Simplify common error messages
            if "No such file" in error_msg:
                error_msg = "File not found or corrupted"
            elif "password" in error_msg.lower():
                error_msg = "File is password protected"
            elif "Invalid file" in error_msg or "not a valid" in error_msg.lower():
                error_msg = "Invalid Excel format"

            file_errors.append({"file": file.name, "error": error_msg})
            log(f"     ❌ Error: {error_msg}")

    # Report file loading summary
    if files_loaded == 0:
        error_summary = "\n".join(f"  • {e['file']}: {e['error']}" for e in file_errors)
        raise ValueError(f"All files failed to load:\n{error_summary}")

    if file_errors:
        warning_count = sum(1 for e in file_errors if e.get("warning"))
        error_count = len(file_errors) - warning_count
        if error_count > 0:
            log(f"⚠️ {error_count} file(s) failed, {files_loaded} loaded successfully")
        if warning_count > 0:
            log(f"⚠️ {warning_count} file(s) have warnings")
    else:
        log(f"✅ Loaded {files_loaded} files")

    # ========== TRANSFORM ==========
    log("🔄 Transforming data...")
    combined = pl.concat(dataframes, how="diagonal")

    # Add Year and Month
    if "Date" in combined.columns:
        combined = combined.with_columns(
            [
                pl.col("Date").dt.year().alias("Year"),
                pl.col("Date").dt.month().alias("Month"),
            ]
        )

    # Normalize Status
    if "Status" in combined.columns:
        combined = combined.with_columns(
            pl.col("Status")
            .map_elements(
                lambda x: status_mapping.get(x, x) if x else None,
                return_dtype=pl.Utf8,
            )
            .alias("Status_Normalized")
        )

    log(f"✅ Combined {len(combined):,} rows")

    # ========== LOAD ==========
    log("💾 Saving partitioned files...")

    total_saved = save_partitioned(combined, output_path)

    # Report data quality issues
    null_dates = (
        combined.filter(pl.col("Year").is_null()).height if "Year" in combined.columns else 0
    )
    if null_dates > 0:
        log(f"⚠️ {null_dates:,} rows with missing dates (excluded from output)")

    log(f"\n✅ Pipeline complete! {total_saved:,} rows saved")

    return {
        "input_files": len(excel_files),
        "files_loaded": files_loaded,
        "file_errors": file_errors,
        "total_rows": len(combined),
        "saved_rows": total_saved,
        "null_dates": null_dates,
        "output_dir": str(output_path),
    }


def generate_metrics(
    data_dir: str,
    progress_callback: Callable[[str], None] = None,
) -> dict:
    """Generate dashboard metrics from processed data.

    Args:
        data_dir: Directory containing processed parquet files
        progress_callback: Optional callback for progress messages

    Returns:
        Dict with monthly metrics for dashboard
    """

    def log(msg: str):
        if progress_callback:
            progress_callback(msg)
        else:
            print(msg)

    path = Path(data_dir)
    all_files = list(path.glob("**/*.parquet"))

    if not all_files:
        raise FileNotFoundError(f"No parquet files found in {data_dir}")

    log(f"📊 Loading {len(all_files)} parquet files...")
    df = pl.concat([pl.read_parquet(f) for f in all_files])

    # Use Status_Normalized if available
    status_col = "Status_Normalized" if "Status_Normalized" in df.columns else "Status"

    # Calculate monthly metrics
    monthly = (
        df.group_by(["Year", "Month"])
        .agg(
            [
                pl.len().alias("total_orders"),
                pl.col(status_col)
                .filter(pl.col(status_col) == "Delivered")
                .len()
                .alias("delivered"),
                pl.col(status_col)
                .filter(pl.col(status_col) == "Cancelled")
                .len()
                .alias("cancelled"),
                pl.col(status_col).filter(pl.col(status_col) == "Returned").len().alias("returned"),
                pl.col(status_col).filter(pl.col(status_col) == "Failed").len().alias("failed"),
            ]
        )
        .sort(["Year", "Month"])
    )

    monthly = monthly.with_columns(
        [
            (pl.col("delivered") / pl.col("total_orders") * 100).round(1).alias("delivery_rate"),
            (pl.col("cancelled") / pl.col("total_orders") * 100).round(1).alias("cancel_rate"),
        ]
    )

    log(f"✅ Generated metrics for {len(monthly)} months")

    return monthly.to_dicts()
