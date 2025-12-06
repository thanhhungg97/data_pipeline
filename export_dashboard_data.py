"""Export Gold layer data as JSON for the React dashboard.

Simply converts Gold layer Parquet files to JSON format.
No aggregation logic here - that's done in the Gold layer.
"""

import json
from pathlib import Path

import polars as pl


def export_to_json(
    gold_dir: str = "data/gold",
    output_file: str = "dashboard/public/data.json",
):
    """Convert Gold layer Parquet files to JSON for React dashboard."""
    print("=" * 50)
    print("EXPORT DASHBOARD DATA")
    print("=" * 50)

    gold_path = Path(gold_dir)
    metrics_path = gold_path / "metrics"

    # Load pre-aggregated metrics from Gold layer
    print("\n[1/2] LOAD GOLD METRICS")
    print("-" * 30)

    # Monthly by source metrics
    monthly_file = metrics_path / "monthly_by_source.parquet"
    if not monthly_file.exists():
        print(f"❌ Not found: {monthly_file}")
        print("   Run 'python main.py' first to generate Gold layer data")
        return

    metrics_df = pl.read_parquet(monthly_file)
    print(f"  ✓ Loaded monthly_by_source.parquet ({len(metrics_df)} rows)")

    # Cancellation reasons
    reasons_file = metrics_path / "cancellation_reasons.parquet"
    if reasons_file.exists():
        reasons_df = pl.read_parquet(reasons_file)
        print(f"  ✓ Loaded cancellation_reasons.parquet ({len(reasons_df)} rows)")
    else:
        reasons_df = pl.DataFrame()
        print("  ⚠ No cancellation_reasons.parquet found")

    # Build JSON structure
    print("\n[2/2] BUILD JSON")
    print("-" * 30)

    # Extract unique sources (excluding "All")
    sources = sorted(metrics_df.filter(pl.col("Source") != "All")["Source"].unique().to_list())

    # Monthly totals (from "All" source)
    all_metrics = metrics_df.filter(pl.col("Source") == "All")
    monthly_data = [
        {
            "Year": row["Year"],
            "Month": row["Month"],
            "total_orders": row["total_orders"],
            "delivered": row["delivered"],
            "cancelled": row["cancelled"],
            "returned": row["returned"],
            "failed": row["failed"],
        }
        for row in all_metrics.iter_rows(named=True)
    ]

    # Per-source metrics
    metrics_data = [
        {
            "Source": row["Source"],
            "Year": row["Year"],
            "Month": row["Month"],
            "total_orders": row["total_orders"],
            "delivered": row["delivered"],
            "cancelled": row["cancelled"],
            "returned": row["returned"],
            "failed": row["failed"],
            "delivery_rate": row["delivery_rate"],
            "cancel_rate": row["cancel_rate"],
        }
        for row in metrics_df.iter_rows(named=True)
    ]

    # Cancellation reasons
    reasons_data = []
    if len(reasons_df) > 0:
        reasons_data = [
            {
                "Year": row["Year"],
                "Month": row["Month"],
                "Reason cancelled": row["Reason cancelled"],
                "count": row["count"],
            }
            for row in reasons_df.iter_rows(named=True)
        ]

    # Write JSON
    data = {
        "monthly": monthly_data,
        "reasons": reasons_data,
        "metrics": metrics_data,
        "sources": sources,
    }

    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f)

    print(f"  ✓ Exported to: {output_file}")

    # Summary
    print("\n" + "=" * 50)
    print("EXPORT COMPLETE")
    print("=" * 50)
    print("\n📊 Summary:")
    print(f"   Months: {len(monthly_data)}")
    print(f"   Sources: {sources}")
    print(f"   Metrics: {len(metrics_data)}")
    print(f"   Reasons: {len(reasons_data)}")


if __name__ == "__main__":
    export_to_json()
