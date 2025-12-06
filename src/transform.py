"""Transform: Clean, aggregate, join data from multiple sources."""
import polars as pl


def clean_dataframe(df: pl.DataFrame) -> pl.DataFrame:
    """Basic cleaning: strip whitespace from strings, drop empty rows."""
    string_cols = [col for col, dtype in zip(df.columns, df.dtypes) if dtype == pl.Utf8]
    
    if string_cols:
        df = df.with_columns([
            pl.col(col).str.strip_chars() for col in string_cols
        ])
    
    # Drop rows where all values are null
    df = df.filter(~pl.all_horizontal(pl.all().is_null()))
    
    return df


def add_year_month(df: pl.DataFrame, date_col: str = "Date") -> pl.DataFrame:
    """Add Year and Month columns from a date column."""
    return df.with_columns([
        pl.col(date_col).dt.year().alias("Year"),
        pl.col(date_col).dt.month().alias("Month"),
    ])


def normalize_status(df: pl.DataFrame) -> pl.DataFrame:
    """Normalize status values across different sources."""
    # Map various status names to standard ones
    status_mapping = {
        # Delivered variants
        "Delivered": "Delivered",
        "Completed": "Delivered",
        "Done": "Delivered",
        "Success": "Delivered",
        
        # Cancelled variants
        "Cancel by cust.": "Cancelled",
        "Cancelled": "Cancelled",
        "Canceled": "Cancelled",
        "Cancel": "Cancelled",
        "Cancelled by customer": "Cancelled",
        
        # Returned variants
        "Returned": "Returned",
        "Return": "Returned",
        "Refunded": "Returned",
        
        # Failed variants
        "Failed delivery": "Failed",
        "Failed": "Failed",
        "Delivery Failed": "Failed",
    }
    
    # Create mapping expression
    if "Status" in df.columns:
        df = df.with_columns(
            pl.col("Status")
            .map_elements(lambda x: status_mapping.get(x, x), return_dtype=pl.Utf8)
            .alias("Status_Normalized")
        )
    
    return df


def aggregate(
    df: pl.DataFrame,
    group_by: str | list[str],
    aggregations: dict[str, str]
) -> pl.DataFrame:
    """Aggregate data by specified columns."""
    agg_map = {
        "sum": pl.sum,
        "mean": pl.mean,
        "min": pl.min,
        "max": pl.max,
        "count": pl.len,
        "first": pl.first,
        "last": pl.last,
    }
    
    agg_exprs = []
    for col, func_name in aggregations.items():
        func = agg_map.get(func_name)
        if func:
            if func_name == "count":
                agg_exprs.append(func().alias(f"{col}_{func_name}"))
            else:
                agg_exprs.append(func(col).alias(f"{col}_{func_name}"))
    
    return df.group_by(group_by).agg(agg_exprs)


def join_dataframes(
    left: pl.DataFrame,
    right: pl.DataFrame,
    on: str | list[str],
    how: str = "left"
) -> pl.DataFrame:
    """Join two DataFrames."""
    return left.join(right, on=on, how=how)


# ============================================================
# MULTI-SOURCE TRANSFORMS
# ============================================================

def transform_source(df: pl.DataFrame, source_name: str) -> pl.DataFrame:
    """Apply source-specific transformations."""
    # Clean
    df = clean_dataframe(df)
    
    # Ensure Source column
    if "Source" not in df.columns:
        df = df.with_columns(pl.lit(source_name).alias("Source"))
    
    # Add Year/Month
    if "Date" in df.columns:
        df = add_year_month(df, "Date")
    
    # Normalize status (optional - keeps original too)
    df = normalize_status(df)
    
    return df


def run_transforms(dataframes: dict[str, pl.DataFrame]) -> dict[str, pl.DataFrame]:
    """
    Main transform function for multi-source data.
    
    Args:
        dataframes: Dict of {source_name: DataFrame}
    
    Returns:
        Dict with:
        - Individual source DataFrames (transformed)
        - Combined "all_sources" DataFrame
    """
    results = {}
    all_dfs = []
    
    # Transform each source
    for source_name, df in dataframes.items():
        print(f"  Transforming {source_name}...")
        transformed = transform_source(df, source_name)
        results[source_name.lower().replace(" ", "_")] = transformed
        all_dfs.append(transformed)
    
    # Combine all sources
    if all_dfs:
        combined = pl.concat(all_dfs, how="diagonal")
        results["all_sources"] = combined
        print(f"  ✅ Combined all sources: {len(combined):,} rows")
    
    return results
