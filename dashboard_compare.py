"""Interactive dashboard with month-over-month and year-over-year comparisons."""
import polars as pl
from pathlib import Path
import json


def load_all_data(processed_dir: str = "data/processed") -> pl.DataFrame:
    """Load all partitioned parquet files (supports multi-source structure)."""
    path = Path(processed_dir)
    all_files = list(path.glob("**/*.parquet"))
    
    if not all_files:
        raise FileNotFoundError(f"No parquet files found in {processed_dir}")
    
    df = pl.concat([pl.read_parquet(f) for f in all_files], how="diagonal")
    print(f"Loaded {len(df):,} rows from {len(all_files)} files")
    return df


def calculate_metrics(df: pl.DataFrame) -> pl.DataFrame:
    """Calculate monthly metrics."""
    monthly = df.group_by(["Year", "Month"]).agg([
        pl.len().alias("total_orders"),
        pl.col("Status").filter(pl.col("Status") == "Delivered").len().alias("delivered"),
        pl.col("Status").filter(pl.col("Status").is_in(["Cancel by cust.", "Cancelled"])).len().alias("cancelled"),
        pl.col("Status").filter(pl.col("Status") == "Returned").len().alias("returned"),
        pl.col("Status").filter(pl.col("Status").is_in(["Failed delivery", "Failed"])).len().alias("failed"),
    ]).sort(["Year", "Month"])
    
    monthly = monthly.with_columns([
        (pl.col("delivered") / pl.col("total_orders") * 100).round(1).alias("delivery_rate"),
        (pl.col("cancelled") / pl.col("total_orders") * 100).round(1).alias("cancel_rate"),
    ])
    
    return monthly


def load_template(template_name: str) -> str:
    """Load HTML template from templates folder."""
    template_path = Path(__file__).parent / "templates" / template_name
    with open(template_path, 'r', encoding='utf-8') as f:
        return f.read()


def load_nav() -> str:
    """Load navigation HTML."""
    nav_path = Path(__file__).parent / "templates" / "nav.html"
    with open(nav_path, 'r', encoding='utf-8') as f:
        return f.read()


def generate_html_dashboard(df: pl.DataFrame, output_file: str = "dashboard_compare.html"):
    """Generate interactive HTML dashboard with comparisons."""
    
    monthly = calculate_metrics(df)
    data_js = monthly.to_dicts()
    
    # Load template
    html_content = load_template("dashboard_compare.html")
    nav_html = load_nav()
    
    # Replace placeholders
    html_content = html_content.replace("{{NAV}}", nav_html)
    html_content = html_content.replace("{{DATA}}", json.dumps(data_js))
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"\n✅ Dashboard saved to: {output_file}")


if __name__ == "__main__":
    df = load_all_data()
    generate_html_dashboard(df)
