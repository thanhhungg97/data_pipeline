"""Export processed data as JSON for the React dashboard.

ETL Pattern:
1. EXTRACT - Scan partitioned directories, load month by month
2. TRANSFORM - Aggregate metrics incrementally
3. LOAD - Write final JSON

Uses Status_Normalized column (standardized by transform step).
"""

import json
from pathlib import Path

import polars as pl
import yaml


def load_config(config_path: str = "config.yaml") -> dict:
    """Load configuration from YAML file."""
    with open(config_path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_status_categories() -> list[str]:
    """Get status categories from config."""
    config = load_config()
    return config.get("status_categories", ["Delivered", "Cancelled", "Returned", "Failed"])


# =============================================================================
# EXTRACT - Load data partition by partition
# =============================================================================


def scan_partitions(processed_dir: str = "data/processed/all_sources") -> list[dict]:
    """Scan partition directories and return list of {year, month, path}."""
    path = Path(processed_dir)
    partitions = []

    for year_dir in sorted(path.iterdir()):
        if not year_dir.is_dir() or not year_dir.name.isdigit():
            continue
        year = int(year_dir.name)

        for month_dir in sorted(year_dir.iterdir()):
            if not month_dir.is_dir() or not month_dir.name.isdigit():
                continue
            month = int(month_dir.name)

            parquet_file = month_dir / "orders.parquet"
            if parquet_file.exists():
                partitions.append({"year": year, "month": month, "path": parquet_file})

    return partitions


def load_partition(partition: dict) -> pl.DataFrame:
    """Load a single partition."""
    return pl.read_parquet(partition["path"])


def get_sources(processed_dir: str = "data/processed") -> list[str]:
    """Get list of source names from directory structure."""
    path = Path(processed_dir)
    sources = []

    for item in sorted(path.iterdir()):
        if item.is_dir() and item.name != "all_sources":
            sources.append(item.name)

    return sources


# =============================================================================
# TRANSFORM - Calculate metrics per partition
# =============================================================================


def aggregate_monthly(df: pl.DataFrame, year: int, month: int) -> dict:
    """Aggregate monthly metrics from a single partition.

    Uses Status_Normalized column (standardized categories from config).
    """
    # Use Status_Normalized if available, fallback to Status
    status_col = "Status_Normalized" if "Status_Normalized" in df.columns else "Status"

    total = len(df)
    delivered = len(df.filter(pl.col(status_col) == "Delivered"))
    cancelled = len(df.filter(pl.col(status_col) == "Cancelled"))
    returned = len(df.filter(pl.col(status_col) == "Returned"))
    failed = len(df.filter(pl.col(status_col) == "Failed"))

    return {
        "Year": year,
        "Month": month,
        "total_orders": total,
        "delivered": delivered,
        "cancelled": cancelled,
        "returned": returned,
        "failed": failed,
    }


def aggregate_reasons(df: pl.DataFrame, year: int, month: int) -> list[dict]:
    """Aggregate cancellation reasons from a single partition."""
    status_col = "Status_Normalized" if "Status_Normalized" in df.columns else "Status"

    cancelled_df = df.filter(pl.col(status_col) == "Cancelled").filter(
        pl.col("Reason cancelled").is_not_null()
    )

    if len(cancelled_df) == 0:
        return []

    reasons = (
        cancelled_df.group_by("Reason cancelled")
        .agg(pl.len().alias("count"))
        .sort("count", descending=True)
    )

    return [
        {"Year": year, "Month": month, "Reason cancelled": row[0], "count": row[1]}
        for row in reasons.iter_rows()
    ]


def aggregate_source_metrics(df: pl.DataFrame, year: int, month: int, source: str) -> dict:
    """Aggregate metrics for a specific source from a partition.

    Uses Status_Normalized column (standardized categories from config).
    """
    total = len(df)
    if total == 0:
        return None

    status_col = "Status_Normalized" if "Status_Normalized" in df.columns else "Status"

    delivered = len(df.filter(pl.col(status_col) == "Delivered"))
    cancelled = len(df.filter(pl.col(status_col) == "Cancelled"))
    returned = len(df.filter(pl.col(status_col) == "Returned"))
    failed = len(df.filter(pl.col(status_col) == "Failed"))

    return {
        "Source": source,
        "Year": year,
        "Month": month,
        "total_orders": total,
        "delivered": delivered,
        "cancelled": cancelled,
        "returned": returned,
        "failed": failed,
        "delivery_rate": round(delivered / total * 100, 1) if total > 0 else 0,
        "cancel_rate": round(cancelled / total * 100, 1) if total > 0 else 0,
    }


# =============================================================================
# LOAD - Build and write final JSON
# =============================================================================


def run_export_pipeline(
    processed_dir: str = "data/processed",
    output_file: str = "dashboard/public/data.json",
):
    """Run the export ETL pipeline."""
    print("=" * 50)
    print("EXPORT DASHBOARD DATA (ETL)")
    print("=" * 50)

    # -----------------------------------------------------
    # EXTRACT - Scan partitions
    # -----------------------------------------------------
    print("\n[1/3] EXTRACT")
    print("-" * 30)

    all_sources_dir = f"{processed_dir}/all_sources"
    partitions = scan_partitions(all_sources_dir)
    sources = get_sources(processed_dir)

    print(f"Found {len(partitions)} month partitions")
    print(f"Sources: {sources}")

    if not partitions:
        print("❌ No partitions found!")
        return

    # -----------------------------------------------------
    # TRANSFORM - Aggregate metrics per partition
    # -----------------------------------------------------
    print("\n[2/3] TRANSFORM")
    print("-" * 30)

    monthly_data = []
    reasons_data = []
    metrics_data = []

    for _i, partition in enumerate(partitions):
        year, month = partition["year"], partition["month"]
        print(f"  Processing {year}/{month:02d}...", end=" ")

        # Load all_sources partition
        df = load_partition(partition)

        # Monthly aggregates
        monthly_data.append(aggregate_monthly(df, year, month))

        # Cancellation reasons
        reasons_data.extend(aggregate_reasons(df, year, month))

        # Source metrics for "All"
        all_metric = aggregate_source_metrics(df, year, month, "All")
        if all_metric:
            metrics_data.append(all_metric)

        # Source metrics per source
        for source in sources:
            source_df = df.filter(pl.col("Source").str.to_lowercase() == source.lower())
            if len(source_df) > 0:
                # Use proper case from data
                actual_source = source_df["Source"].unique().to_list()[0]
                metric = aggregate_source_metrics(source_df, year, month, actual_source)
                if metric:
                    metrics_data.append(metric)

        print(f"✓ ({len(df):,} rows)")

    # Get unique source names (proper case)
    source_names = sorted({m["Source"] for m in metrics_data if m["Source"] != "All"})

    print(f"\n  ✅ Processed {len(partitions)} months")
    print(f"     Monthly records: {len(monthly_data)}")
    print(f"     Reason records: {len(reasons_data)}")
    print(f"     Metric records: {len(metrics_data)}")

    # -----------------------------------------------------
    # LOAD - Write JSON
    # -----------------------------------------------------
    print("\n[3/3] LOAD")
    print("-" * 30)

    data = {
        "monthly": monthly_data,
        "reasons": reasons_data,
        "metrics": metrics_data,
        "sources": source_names,
    }

    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f)

    print(f"  ✅ Exported to: {output_file}")

    # Summary
    print("\n" + "=" * 50)
    print("EXPORT COMPLETE")
    print("=" * 50)
    print("\n📊 Summary:")
    print(f"   Months: {len(monthly_data)}")
    print(f"   Sources: {source_names}")
    print(f"   Metrics: {len(metrics_data)}")


if __name__ == "__main__":
    run_export_pipeline()
