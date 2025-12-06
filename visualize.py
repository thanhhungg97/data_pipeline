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


def calculate_monthly_detailed(df: pl.DataFrame) -> list[dict]:
    """Calculate detailed monthly metrics for filtering."""
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


def calculate_cancel_reasons_by_month(df: pl.DataFrame) -> list[dict]:
    """Get cancellation reasons grouped by year/month."""
    reasons = (
        df.filter(pl.col("Status").is_in(["Cancel by cust.", "Cancelled"]))
        .filter(pl.col("Reason cancelled").is_not_null())
        .group_by(["Year", "Month", "Reason cancelled"])
        .agg(pl.len().alias("count"))
        .sort(["Year", "Month", "count"], descending=[False, False, True])
    )

    return reasons.to_dicts()


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

    monthly = calculate_monthly_detailed(df)
    reasons = calculate_cancel_reasons_by_month(df)

    # Load template
    html_content = load_template("dashboard_overview.html")
    nav_html = load_nav()

    # Replace placeholders
    html_content = html_content.replace("{{NAV}}", nav_html)
    html_content = html_content.replace("{{MONTHLY}}", json.dumps(monthly))
    html_content = html_content.replace("{{REASONS}}", json.dumps(reasons))

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(html_content)

    print(f"\n✅ Overview dashboard saved to: {output_file}")


def print_summary(df: pl.DataFrame):
    """Print quick summary stats."""
    total = len(df)
    delivered = df.filter(pl.col("Status") == "Delivered").height
    cancelled = df.filter(pl.col("Status").is_in(["Cancel by cust.", "Cancelled"])).height
    returned = df.filter(pl.col("Status") == "Returned").height

    date_min = df["Date"].min()
    date_max = df["Date"].max()

    print("\n" + "=" * 50)
    print("📊 QUICK SUMMARY")
    print("=" * 50)
    print(f"Total Orders: {total:,}")
    print(f"Date Range: {date_min} to {date_max}")
    print(f"\n✅ Delivery Rate: {round(delivered / total * 100, 1)}%")
    print(f"❌ Cancel Rate: {round(cancelled / total * 100, 1)}%")
    print(f"↩️  Return Rate: {round(returned / total * 100, 1)}%")


if __name__ == "__main__":
    df = load_all_data()
    print_summary(df)
    create_dashboard(df)
