"""Visualize Shopee order data."""
import polars as pl
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path


def load_all_data(processed_dir: str = "data/processed") -> pl.DataFrame:
    """Load all partitioned parquet files (supports multi-source structure)."""
    path = Path(processed_dir)
    all_files = list(path.glob("**/*.parquet"))
    
    if not all_files:
        raise FileNotFoundError(f"No parquet files found in {processed_dir}")
    
    # Use diagonal concat to handle different schemas across sources
    df = pl.concat([pl.read_parquet(f) for f in all_files], how="diagonal")
    print(f"Loaded {len(df):,} rows from {len(all_files)} files")
    return df


def create_dashboard(df: pl.DataFrame, output_file: str = "dashboard.html"):
    """Create an interactive HTML dashboard."""
    
    # Prepare data
    df = df.with_columns([
        pl.concat_str([pl.col("Year").cast(pl.Utf8), pl.lit("-"), 
                       pl.col("Month").cast(pl.Utf8).str.zfill(2)]).alias("YearMonth")
    ])
    
    # 1. Monthly order counts
    monthly = df.group_by("YearMonth").agg(pl.count().alias("Orders")).sort("YearMonth")
    
    # 2. Status breakdown
    status_counts = df.group_by("Status").agg(pl.count().alias("Count")).sort("Count", descending=True)
    
    # 3. Monthly orders by status
    monthly_status = df.group_by(["YearMonth", "Status"]).agg(pl.count().alias("Orders")).sort("YearMonth")
    
    # 4. Cancellation reasons (only for cancelled orders)
    cancel_reasons = (
        df.filter(pl.col("Status") == "Cancel by cust.")
        .filter(pl.col("Reason cancelled").is_not_null())
        .group_by("Reason cancelled")
        .agg(pl.count().alias("Count"))
        .sort("Count", descending=True)
        .head(10)
    )
    
    # Create subplots
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=(
            "📈 Monthly Order Volume",
            "📊 Order Status Distribution", 
            "📉 Monthly Orders by Status",
            "❌ Top Cancellation Reasons"
        ),
        specs=[
            [{"type": "scatter"}, {"type": "pie"}],
            [{"type": "bar"}, {"type": "bar"}]
        ],
        vertical_spacing=0.12,
        horizontal_spacing=0.1
    )
    
    # Plot 1: Monthly orders line chart
    fig.add_trace(
        go.Scatter(
            x=monthly["YearMonth"].to_list(),
            y=monthly["Orders"].to_list(),
            mode="lines+markers",
            name="Orders",
            line=dict(color="#636EFA", width=3),
            marker=dict(size=8)
        ),
        row=1, col=1
    )
    
    # Plot 2: Status pie chart
    colors = ["#00CC96", "#EF553B", "#FFA15A", "#AB63FA"]
    fig.add_trace(
        go.Pie(
            labels=status_counts["Status"].to_list(),
            values=status_counts["Count"].to_list(),
            marker=dict(colors=colors),
            textinfo="label+percent",
            hole=0.4
        ),
        row=1, col=2
    )
    
    # Plot 3: Stacked bar by status
    statuses = df["Status"].unique().to_list()
    color_map = {
        "Delivered": "#00CC96",
        "Cancel by cust.": "#EF553B", 
        "Returned": "#FFA15A",
        "Failed delivery": "#AB63FA"
    }
    
    for status in statuses:
        status_data = monthly_status.filter(pl.col("Status") == status)
        fig.add_trace(
            go.Bar(
                x=status_data["YearMonth"].to_list(),
                y=status_data["Orders"].to_list(),
                name=status,
                marker_color=color_map.get(status, "#636EFA")
            ),
            row=2, col=1
        )
    
    # Plot 4: Cancellation reasons
    fig.add_trace(
        go.Bar(
            x=cancel_reasons["Count"].to_list(),
            y=cancel_reasons["Reason cancelled"].to_list(),
            orientation="h",
            marker_color="#EF553B",
            showlegend=False
        ),
        row=2, col=2
    )
    
    # Update layout
    fig.update_layout(
        title=dict(
            text="🛒 Shopee Orders Dashboard",
            font=dict(size=24)
        ),
        height=900,
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        barmode="stack",
        template="plotly_white"
    )
    
    # Update axes
    fig.update_xaxes(title_text="Month", row=1, col=1, tickangle=45)
    fig.update_yaxes(title_text="Orders", row=1, col=1)
    fig.update_xaxes(title_text="Month", row=2, col=1, tickangle=45)
    fig.update_yaxes(title_text="Orders", row=2, col=1)
    fig.update_xaxes(title_text="Count", row=2, col=2)
    
    # Save to HTML
    fig.write_html(output_file)
    print(f"\n✅ Dashboard saved to: {output_file}")
    print("Open this file in your browser to view the interactive dashboard.")
    
    return fig


def print_summary(df: pl.DataFrame):
    """Print quick summary stats."""
    print("\n" + "=" * 50)
    print("📊 QUICK SUMMARY")
    print("=" * 50)
    print(f"Total Orders: {len(df):,}")
    print(f"Date Range: {df['Date'].min()} to {df['Date'].max()}")
    print(f"\nStatus Breakdown:")
    
    status_counts = df.group_by("Status").agg(pl.count().alias("Count")).sort("Count", descending=True)
    for row in status_counts.iter_rows(named=True):
        pct = row["Count"] / len(df) * 100
        print(f"  {row['Status']}: {row['Count']:,} ({pct:.1f}%)")
    
    # Delivery rate
    delivered = df.filter(pl.col("Status") == "Delivered").height
    delivery_rate = delivered / len(df) * 100
    print(f"\n✅ Delivery Success Rate: {delivery_rate:.1f}%")


if __name__ == "__main__":
    df = load_all_data()
    print_summary(df)
    create_dashboard(df)

