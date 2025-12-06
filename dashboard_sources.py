"""Multi-source comparison dashboard."""
import polars as pl
from pathlib import Path
import json


def load_all_data(processed_dir: str = "data/processed") -> pl.DataFrame:
    """Load all partitioned parquet files from all sources."""
    path = Path(processed_dir)
    all_files = list(path.glob("**/*.parquet"))
    
    if not all_files:
        raise FileNotFoundError(f"No parquet files found in {processed_dir}")
    
    df = pl.concat([pl.read_parquet(f) for f in all_files], how="diagonal")
    print(f"Loaded {len(df):,} rows from {len(all_files)} files")
    return df


def calculate_source_metrics(df: pl.DataFrame) -> list[dict]:
    """Calculate metrics grouped by Source, Year, Month."""
    metrics = df.group_by(["Source", "Year", "Month"]).agg([
        pl.len().alias("total_orders"),
        pl.col("Status").filter(pl.col("Status") == "Delivered").len().alias("delivered"),
        pl.col("Status").filter(pl.col("Status").is_in(["Cancel by cust.", "Cancelled"])).len().alias("cancelled"),
        pl.col("Status").filter(pl.col("Status") == "Returned").len().alias("returned"),
        pl.col("Status").filter(pl.col("Status").is_in(["Failed delivery", "Failed"])).len().alias("failed"),
    ]).sort(["Source", "Year", "Month"])
    
    metrics = metrics.with_columns([
        (pl.col("delivered") / pl.col("total_orders") * 100).round(1).alias("delivery_rate"),
        (pl.col("cancelled") / pl.col("total_orders") * 100).round(1).alias("cancel_rate"),
    ])
    
    return metrics.to_dicts()


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


def generate_source_comparison_dashboard(
    df: pl.DataFrame, 
    output_file: str = "dashboard_sources.html"
):
    """Generate interactive HTML dashboard for comparing sources."""
    
    sources = df["Source"].drop_nulls().unique().sort().to_list()
    metrics = calculate_source_metrics(df)
    
    # Load template
    html_content = load_template("dashboard_sources.html")
    nav_html = load_nav()
    
    # Replace placeholders
    html_content = html_content.replace("{{NAV}}", nav_html)
    html_content = html_content.replace("{{METRICS}}", json.dumps(metrics))
    html_content = html_content.replace("{{SOURCES}}", json.dumps(sources))
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"\n✅ Source comparison dashboard saved to: {output_file}")
    return output_file


if __name__ == "__main__":
    df = load_all_data()
    
    sources = df["Source"].drop_nulls().unique().to_list()
    print(f"\nSources found: {sources}")
    
    generate_source_comparison_dashboard(df)
