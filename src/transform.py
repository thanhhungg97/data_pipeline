"""Transform: Clean, aggregate, join data."""
import polars as pl


def clean_dataframe(df: pl.DataFrame) -> pl.DataFrame:
    """Basic cleaning: strip whitespace from strings, drop empty rows."""
    # Strip whitespace from string columns
    string_cols = [col for col, dtype in zip(df.columns, df.dtypes) if dtype == pl.Utf8]
    
    if string_cols:
        df = df.with_columns([
            pl.col(col).str.strip_chars() for col in string_cols
        ])
    
    # Drop rows where all values are null
    df = df.filter(~pl.all_horizontal(pl.all().is_null()))
    
    return df


def aggregate(
    df: pl.DataFrame,
    group_by: str | list[str],
    aggregations: dict[str, str]
) -> pl.DataFrame:
    """
    Aggregate data by specified columns.
    
    Args:
        df: Input DataFrame
        group_by: Column(s) to group by
        aggregations: Dict of {column: agg_function}
            agg_function can be: sum, mean, min, max, count, first, last
    
    Example:
        aggregate(df, "region", {"revenue": "sum", "quantity": "mean"})
    """
    agg_map = {
        "sum": pl.sum,
        "mean": pl.mean,
        "min": pl.min,
        "max": pl.max,
        "count": pl.count,
        "first": pl.first,
        "last": pl.last,
    }
    
    agg_exprs = []
    for col, func_name in aggregations.items():
        func = agg_map.get(func_name)
        if func:
            agg_exprs.append(func(col).alias(f"{col}_{func_name}"))
    
    return df.group_by(group_by).agg(agg_exprs)


def join_dataframes(
    left: pl.DataFrame,
    right: pl.DataFrame,
    on: str | list[str],
    how: str = "left"
) -> pl.DataFrame:
    """
    Join two DataFrames.
    
    Args:
        left: Left DataFrame
        right: Right DataFrame
        on: Column(s) to join on
        how: Join type - left, right, inner, outer, cross
    """
    return left.join(right, on=on, how=how)


def filter_data(df: pl.DataFrame, conditions: list[pl.Expr]) -> pl.DataFrame:
    """Apply multiple filter conditions."""
    for condition in conditions:
        df = df.filter(condition)
    return df


# ============================================================
# CUSTOMIZE YOUR TRANSFORMS HERE
# ============================================================

def add_year_month(df: pl.DataFrame, date_col: str = "Date") -> pl.DataFrame:
    """Add Year and Month columns from a date column."""
    return df.with_columns([
        pl.col(date_col).dt.year().alias("Year"),
        pl.col(date_col).dt.month().alias("Month"),
    ])


def run_transforms(dataframes: dict[str, pl.DataFrame]) -> dict[str, pl.DataFrame]:
    """
    Main transform function - customize this for your data.
    
    Args:
        dataframes: Dict of {name: DataFrame} from extract step
    
    Returns:
        Dict of {name: DataFrame} to be loaded
    """
    results = {}
    
    # 1. Clean all dataframes
    for name, df in dataframes.items():
        dataframes[name] = clean_dataframe(df)
    
    # 2. Combine all files and add Year/Month columns
    all_dfs = list(dataframes.values())
    
    if all_dfs:
        # Stack all dataframes (handles mismatched columns)
        combined = pl.concat(all_dfs, how="diagonal")
        
        # Add Year and Month columns
        combined = add_year_month(combined, "Date")
        
        results["shopee_all_orders"] = combined
    
    return results

