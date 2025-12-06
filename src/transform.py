"""Transform: Clean, normalize, aggregate data from multiple sources.

Standard Output Schema:
- Date (Datetime) - normalized to datetime
- Source (String) - source name
- Order ID (String)
- Status (String) - original status
- Status_Normalized (String) - standardized: Delivered, Cancelled, Returned, Failed
- Reason cancelled (String)
- Year (Int32)
- Month (Int32)
"""
import polars as pl


# Standard schema for processed data
STANDARD_SCHEMA = {
    "Date": pl.Datetime,
    "Source": pl.Utf8,
    "Order ID": pl.Utf8,
    "Status": pl.Utf8,
    "Status_Normalized": pl.Utf8,
    "Reason cancelled": pl.Utf8,
    "Year": pl.Int32,
    "Month": pl.Int32,
}

# Status normalization mapping
STATUS_MAPPING = {
    # Delivered variants
    "Delivered": "Delivered",
    "Completed": "Delivered",
    "Done": "Delivered",
    "Success": "Delivered",
    "Giao thành công": "Delivered",
    
    # Cancelled variants
    "Cancel by cust.": "Cancelled",
    "Cancelled": "Cancelled",
    "Canceled": "Cancelled",
    "Cancel": "Cancelled",
    "Cancelled by customer": "Cancelled",
    "Đã hủy": "Cancelled",
    
    # Returned variants
    "Returned": "Returned",
    "Return": "Returned",
    "Refunded": "Returned",
    "Hoàn trả": "Returned",
    
    # Failed variants
    "Failed delivery": "Failed",
    "Failed": "Failed",
    "Delivery Failed": "Failed",
    "Giao thất bại": "Failed",
}


def normalize_date(df: pl.DataFrame) -> pl.DataFrame:
    """Normalize Date column to Datetime type."""
    if "Date" in df.columns:
        df = df.with_columns(
            pl.col("Date").cast(pl.Datetime).alias("Date")
        )
    return df


def normalize_status(df: pl.DataFrame) -> pl.DataFrame:
    """Normalize status values to standard categories."""
    if "Status" not in df.columns:
        return df
    
    df = df.with_columns(
        pl.col("Status")
        .map_elements(lambda x: STATUS_MAPPING.get(x, x) if x else None, return_dtype=pl.Utf8)
        .alias("Status_Normalized")
    )
    return df


def add_year_month(df: pl.DataFrame) -> pl.DataFrame:
    """Add Year and Month columns from Date."""
    if "Date" not in df.columns:
        return df
    
    return df.with_columns([
        pl.col("Date").dt.year().cast(pl.Int32).alias("Year"),
        pl.col("Date").dt.month().cast(pl.Int32).alias("Month"),
    ])


def ensure_source(df: pl.DataFrame, source_name: str) -> pl.DataFrame:
    """Set Source column to the data source (folder name), overwriting any existing value."""
    # Always set Source to the folder/data source name, not the value in the file
    df = df.with_columns(pl.lit(source_name).alias("Source"))
    return df


def clean_strings(df: pl.DataFrame) -> pl.DataFrame:
    """Strip whitespace from string columns."""
    string_cols = [col for col, dtype in zip(df.columns, df.dtypes) if dtype == pl.Utf8]
    
    if string_cols:
        df = df.with_columns([
            pl.col(col).str.strip_chars() for col in string_cols
        ])
    return df


def drop_empty_rows(df: pl.DataFrame) -> pl.DataFrame:
    """Drop rows where all values are null."""
    return df.filter(~pl.all_horizontal(pl.all().is_null()))


def normalize_dataframe(df: pl.DataFrame, source_name: str) -> pl.DataFrame:
    """
    Apply all normalizations to a DataFrame.
    
    Steps:
    1. Normalize Date to Datetime
    2. Ensure Source column
    3. Clean string values
    4. Normalize Status
    5. Add Year/Month
    6. Drop empty rows
    """
    df = normalize_date(df)
    df = ensure_source(df, source_name)
    df = clean_strings(df)
    df = normalize_status(df)
    df = add_year_month(df)
    df = drop_empty_rows(df)
    
    return df


def run_transforms(dataframes: dict[str, pl.DataFrame]) -> dict[str, pl.DataFrame]:
    """
    Main transform function for multi-source data.
    
    Args:
        dataframes: Dict of {source_name: DataFrame}
    
    Returns:
        Dict with:
        - Individual source DataFrames (normalized)
        - Combined "all_sources" DataFrame
    """
    results = {}
    all_dfs = []
    
    for source_name, df in dataframes.items():
        print(f"  Normalizing {source_name}...")
        normalized = normalize_dataframe(df, source_name)
        results[source_name.lower().replace(" ", "_")] = normalized
        all_dfs.append(normalized)
    
    # Combine all sources
    if all_dfs:
        combined = pl.concat(all_dfs, how="diagonal")
        results["all_sources"] = combined
        print(f"  ✅ Combined all sources: {len(combined):,} rows")
    
    return results


# ============================================================
# UTILITY FUNCTIONS (for custom transforms)
# ============================================================

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
