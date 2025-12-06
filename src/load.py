"""Load: Save processed data to destination (with multi-source support)."""
import polars as pl
from pathlib import Path


def save_parquet(df: pl.DataFrame, output_path: str | Path) -> None:
    """Save DataFrame as Parquet (fastest for future reads)."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(output_path)
    print(f"  Saved {len(df):,} rows to {output_path}")


def save_csv(df: pl.DataFrame, output_path: str | Path) -> None:
    """Save DataFrame as CSV."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_csv(output_path)
    print(f"  Saved {len(df):,} rows to {output_path}")


def save_partitioned(
    df: pl.DataFrame,
    output_dir: str | Path,
    filename: str = "orders",
    format: str = "parquet"
) -> None:
    """
    Save DataFrame partitioned by Year/Month into folder hierarchy.
    
    Output structure: output_dir/year/month/filename.parquet
    Example: data/processed/2024/01/orders.parquet
    """
    output_path = Path(output_dir)
    save_func = save_parquet if format == "parquet" else save_csv
    ext = ".parquet" if format == "parquet" else ".csv"
    
    # Filter out rows with null Year/Month
    if "Year" not in df.columns or "Month" not in df.columns:
        print(f"  ⚠️  No Year/Month columns, saving as single file")
        save_func(df, output_path / f"{filename}{ext}")
        return
    
    df_valid = df.filter(pl.col("Year").is_not_null() & pl.col("Month").is_not_null())
    
    # Get unique year/month combinations
    unique_periods = df_valid.select(["Year", "Month"]).unique().sort(["Year", "Month"])
    
    total_files = 0
    total_rows = 0
    for row in unique_periods.iter_rows(named=True):
        year = int(row["Year"])
        month = int(row["Month"])
        
        # Filter data for this year/month
        partition = df_valid.filter(
            (pl.col("Year") == year) & (pl.col("Month") == month)
        )
        
        # Create path: output_dir/year/month/filename.ext
        partition_path = output_path / str(year) / f"{month:02d}" / f"{filename}{ext}"
        save_func(partition, partition_path)
        total_files += 1
        total_rows += len(partition)
    
    print(f"  ✅ Saved {total_rows:,} rows across {total_files} partitioned files")


def save_all(
    dataframes: dict[str, pl.DataFrame],
    output_dir: str | Path,
    format: str = "parquet",
    partition_by_date: bool = False
) -> None:
    """Save all DataFrames to the output directory."""
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    for name, df in dataframes.items():
        print(f"\n📁 Saving {name}...")
        
        if partition_by_date and "Year" in df.columns and "Month" in df.columns:
            # Create source-specific subdirectory
            source_dir = output_path / name
            save_partitioned(df, source_dir, filename="orders", format=format)
        else:
            save_func = save_parquet if format == "parquet" else save_csv
            ext = ".parquet" if format == "parquet" else ".csv"
            save_func(df, output_path / f"{name}{ext}")
