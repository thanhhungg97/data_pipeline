"""Export processed data as JSON for the React dashboard."""

import json
from pathlib import Path

import polars as pl


def load_all_data(processed_dir: str = "data/processed") -> pl.DataFrame:
    """Load all partitioned parquet files."""
    path = Path(processed_dir)
    all_files = list(path.glob("**/*.parquet"))

    if not all_files:
        raise FileNotFoundError(f"No parquet files found in {processed_dir}")

    df = pl.concat([pl.read_parquet(f) for f in all_files], how="diagonal")
    print(f"Loaded {len(df):,} rows from {len(all_files)} files")
    return df


def calculate_monthly_detailed(df: pl.DataFrame) -> list[dict]:
    """Calculate detailed monthly metrics for overview."""
    monthly = (
        df.group_by(["Year", "Month"])
        .agg(
            [
                pl.len().alias("total_orders"),
                pl.col("Status").filter(pl.col("Status") == "Delivered").len().alias("delivered"),
                pl.col("Status")
                .filter(pl.col("Status").is_in(["Cancel by cust.", "Cancelled"]))
                .len()
                .alias("cancelled"),
                pl.col("Status").filter(pl.col("Status") == "Returned").len().alias("returned"),
                pl.col("Status")
                .filter(pl.col("Status").is_in(["Failed delivery", "Failed"]))
                .len()
                .alias("failed"),
            ]
        )
        .sort(["Year", "Month"])
    )

    return monthly.to_dicts()


def calculate_cancel_reasons(df: pl.DataFrame) -> list[dict]:
    """Get cancellation reasons grouped by year/month."""
    reasons = (
        df.filter(pl.col("Status").is_in(["Cancel by cust.", "Cancelled"]))
        .filter(pl.col("Reason cancelled").is_not_null())
        .group_by(["Year", "Month", "Reason cancelled"])
        .agg(pl.len().alias("count"))
        .sort(["Year", "Month", "count"], descending=[False, False, True])
    )

    return reasons.to_dicts()


def calculate_source_metrics(df: pl.DataFrame) -> tuple[list[dict], list[str]]:
    """Calculate metrics per source + All combined."""
    results = []
    sources = df["Source"].drop_nulls().unique().sort().to_list()

    # Calculate for each source
    for source in sources:
        source_df = df.filter(pl.col("Source") == source)
        metrics = (
            source_df.group_by(["Year", "Month"])
            .agg(
                [
                    pl.len().alias("total_orders"),
                    pl.col("Status")
                    .filter(pl.col("Status") == "Delivered")
                    .len()
                    .alias("delivered"),
                    pl.col("Status")
                    .filter(pl.col("Status").is_in(["Cancel by cust.", "Cancelled"]))
                    .len()
                    .alias("cancelled"),
                    pl.col("Status").filter(pl.col("Status") == "Returned").len().alias("returned"),
                    pl.col("Status")
                    .filter(pl.col("Status").is_in(["Failed delivery", "Failed"]))
                    .len()
                    .alias("failed"),
                ]
            )
            .sort(["Year", "Month"])
        )

        metrics = metrics.with_columns(
            [
                pl.lit(source).alias("Source"),
                (pl.col("delivered") / pl.col("total_orders") * 100)
                .round(1)
                .alias("delivery_rate"),
                (pl.col("cancelled") / pl.col("total_orders") * 100).round(1).alias("cancel_rate"),
            ]
        )

        results.extend(metrics.to_dicts())

    # Calculate "All" combined
    all_metrics = (
        df.group_by(["Year", "Month"])
        .agg(
            [
                pl.len().alias("total_orders"),
                pl.col("Status").filter(pl.col("Status") == "Delivered").len().alias("delivered"),
                pl.col("Status")
                .filter(pl.col("Status").is_in(["Cancel by cust.", "Cancelled"]))
                .len()
                .alias("cancelled"),
                pl.col("Status").filter(pl.col("Status") == "Returned").len().alias("returned"),
                pl.col("Status")
                .filter(pl.col("Status").is_in(["Failed delivery", "Failed"]))
                .len()
                .alias("failed"),
            ]
        )
        .sort(["Year", "Month"])
    )

    all_metrics = all_metrics.with_columns(
        [
            pl.lit("All").alias("Source"),
            (pl.col("delivered") / pl.col("total_orders") * 100).round(1).alias("delivery_rate"),
            (pl.col("cancelled") / pl.col("total_orders") * 100).round(1).alias("cancel_rate"),
        ]
    )

    results.extend(all_metrics.to_dicts())

    return results, sources


def export_dashboard_data(output_file: str = "dashboard/public/data.json"):
    """Export all dashboard data to a single JSON file."""
    print("Loading data...")
    df = load_all_data()

    print("Calculating monthly data...")
    monthly = calculate_monthly_detailed(df)

    print("Calculating cancellation reasons...")
    reasons = calculate_cancel_reasons(df)

    print("Calculating source metrics...")
    metrics, sources = calculate_source_metrics(df)

    data = {
        "monthly": monthly,
        "reasons": reasons,
        "metrics": metrics,
        "sources": sources,
    }

    # Ensure output directory exists
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Write JSON
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f)

    print(f"\n✅ Dashboard data exported to: {output_file}")
    print(f"   Monthly data points: {len(monthly)}")
    print(f"   Cancellation reasons: {len(reasons)}")
    print(f"   Source metrics: {len(metrics)}")
    print(f"   Sources: {sources}")


if __name__ == "__main__":
    export_dashboard_data()
