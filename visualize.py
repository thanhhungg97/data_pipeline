"""Visualize order data - Overview dashboard."""

import json
from pathlib import Path

import polars as pl


def load_all_data(processed_dir: str = "data/processed") -> pl.DataFrame:
    """Load all partitioned parquet files (data is already normalized)."""
    path = Path(processed_dir)
    all_files = list(path.glob("**/*.parquet"))

    if not all_files:
        raise FileNotFoundError(f"No parquet files found in {processed_dir}")

    df = pl.concat([pl.read_parquet(f) for f in all_files], how="diagonal")
    print(f"Loaded {len(df):,} rows from {len(all_files)} files")
    return df


def calculate_summary(df: pl.DataFrame) -> dict:
    """Calculate overall summary statistics."""
    total = len(df)
    delivered = df.filter(pl.col("Status") == "Delivered").height
    cancelled = df.filter(pl.col("Status").is_in(["Cancel by cust.", "Cancelled"])).height
    returned = df.filter(pl.col("Status") == "Returned").height
    failed = df.filter(pl.col("Status").is_in(["Failed delivery", "Failed"])).height

    date_min = df["Date"].min()
    date_max = df["Date"].max()

    return {
        "total": total,
        "delivered": delivered,
        "cancelled": cancelled,
        "returned": returned,
        "failed": failed,
        "delivery_rate": round(delivered / total * 100, 1),
        "cancel_rate": round(cancelled / total * 100, 1),
        "return_rate": round(returned / total * 100, 1),
        "failed_rate": round(failed / total * 100, 1),
        "date_range": f"{date_min} to {date_max}" if date_min else "N/A",
    }


def calculate_monthly(df: pl.DataFrame) -> list[dict]:
    """Calculate monthly metrics."""
    monthly = (
        df.group_by(["Year", "Month"])
        .agg(
            [
                pl.len().alias("total_orders"),
            ]
        )
        .sort(["Year", "Month"])
    )

    return monthly.to_dicts()


def calculate_cancel_reasons(df: pl.DataFrame, top_n: int = 10) -> list[dict]:
    """Get top cancellation reasons."""
    reasons = (
        df.filter(pl.col("Status").is_in(["Cancel by cust.", "Cancelled"]))
        .filter(pl.col("Reason cancelled").is_not_null())
        .group_by("Reason cancelled")
        .agg(pl.len().alias("count"))
        .sort("count", descending=True)
        .head(top_n)
    )

    return [{"reason": r["Reason cancelled"], "count": r["count"]} for r in reasons.to_dicts()]


def load_template(template_name: str) -> str:
    """Load HTML template from templates folder."""
    template_path = Path(__file__).parent / "templates" / template_name
    with open(template_path, encoding="utf-8") as f:
        return f.read()


def load_nav() -> str:
    """Load navigation HTML."""
    nav_path = Path(__file__).parent / "templates" / "nav.html"
    with open(nav_path, encoding="utf-8") as f:
        return f.read()


def create_dashboard(df: pl.DataFrame, output_file: str = "dashboard.html"):
    """Create an interactive HTML dashboard."""

    summary = calculate_summary(df)
    monthly = calculate_monthly(df)
    reasons = calculate_cancel_reasons(df)

    # Load template
    html_content = load_template("dashboard_overview.html")
    nav_html = load_nav()

    # Replace placeholders
    html_content = html_content.replace("{{NAV}}", nav_html)
    html_content = html_content.replace("{{SUMMARY}}", json.dumps(summary))
    html_content = html_content.replace("{{MONTHLY}}", json.dumps(monthly))
    html_content = html_content.replace("{{REASONS}}", json.dumps(reasons))

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(html_content)

    print(f"\n✅ Overview dashboard saved to: {output_file}")


def print_summary(df: pl.DataFrame):
    """Print quick summary stats."""
    summary = calculate_summary(df)

    print("\n" + "=" * 50)
    print("📊 QUICK SUMMARY")
    print("=" * 50)
    print(f"Total Orders: {summary['total']:,}")
    print(f"Date Range: {summary['date_range']}")
    print(f"\n✅ Delivery Rate: {summary['delivery_rate']}%")
    print(f"❌ Cancel Rate: {summary['cancel_rate']}%")
    print(f"↩️  Return Rate: {summary['return_rate']}%")


if __name__ == "__main__":
    df = load_all_data()
    print_summary(df)
    create_dashboard(df)
