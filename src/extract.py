"""Extract: Read data from multiple sources (Excel files)."""
import polars as pl
from pathlib import Path
import yaml


def load_config(config_path: str = "config.yaml") -> dict:
    """Load configuration from YAML file."""
    with open(config_path) as f:
        return yaml.safe_load(f)


def read_excel(file_path: str | Path, sheet_name: str = None) -> pl.DataFrame:
    """Read an Excel file into a Polars DataFrame."""
    df = pl.read_excel(file_path, sheet_name=sheet_name)
    print(f"  Extracted {len(df):,} rows from {Path(file_path).name}")
    return df


def read_source_files(
    input_dir: str | Path,
    pattern: str,
    source_name: str
) -> pl.DataFrame | None:
    """Read all files matching pattern for a specific source."""
    input_path = Path(input_dir)
    files = list(input_path.glob(pattern))
    
    if not files:
        return None
    
    print(f"\n📂 Reading {source_name} files ({len(files)} files)...")
    
    dataframes = []
    for file in files:
        df = read_excel(file)
        # Ensure Source column exists
        if "Source" not in df.columns:
            df = df.with_columns(pl.lit(source_name).alias("Source"))
        dataframes.append(df)
    
    if dataframes:
        combined = pl.concat(dataframes, how="diagonal")
        print(f"  ✅ {source_name}: {len(combined):,} total rows")
        return combined
    
    return None


def extract_all_sources(config: dict = None) -> dict[str, pl.DataFrame]:
    """
    Extract data from all configured sources.
    
    Returns:
        Dict of {source_name: DataFrame}
    """
    if config is None:
        config = load_config()
    
    input_dir = config["paths"]["input_dir"]
    sources_config = config.get("sources", {})
    
    results = {}
    
    for source_key, source_conf in sources_config.items():
        pattern = source_conf.get("pattern", f"{source_key}*.xlsx")
        source_name = source_conf.get("source_name", source_key.title())
        
        df = read_source_files(input_dir, pattern, source_name)
        if df is not None:
            results[source_name] = df
    
    # Also check for any unmatched Excel files
    input_path = Path(input_dir)
    all_excel = set(input_path.glob("*.xlsx")) | set(input_path.glob("*.xls"))
    matched_patterns = [s.get("pattern", "") for s in sources_config.values()]
    
    for pattern in matched_patterns:
        for f in input_path.glob(pattern):
            all_excel.discard(f)
    
    if all_excel:
        print(f"\n⚠️  Unmatched files (add source config for these):")
        for f in list(all_excel)[:5]:
            print(f"    - {f.name}")
    
    print(f"\n📊 Loaded {len(results)} sources: {list(results.keys())}")
    return results


# Backward compatibility
def read_all_excel_files(input_dir: str | Path) -> dict[str, pl.DataFrame]:
    """Read all Excel files from a directory (legacy function)."""
    input_path = Path(input_dir)
    dataframes = {}
    
    for file in input_path.glob("*.xlsx"):
        name = file.stem
        dataframes[name] = read_excel(file)
    
    for file in input_path.glob("*.xls"):
        name = file.stem
        dataframes[name] = read_excel(file)
    
    print(f"Loaded {len(dataframes)} files: {list(dataframes.keys())}")
    return dataframes
