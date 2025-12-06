"""Extract: Read data from multiple sources (Excel files).

Supports two methods:
1. Config-based (config.yaml) - for custom patterns/column mappings
2. Folder-based auto-detect - just create data/raw/{source_name}/

Priority: Config sources first, then auto-detect folders not in config.
"""
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
    
    # Normalize Date column to datetime
    if "Date" in df.columns:
        df = df.with_columns(pl.col("Date").cast(pl.Datetime).alias("Date"))
    
    print(f"    {Path(file_path).name}: {len(df):,} rows")
    return df


def read_source_files(
    input_dir: Path,
    pattern: str,
    source_name: str
) -> pl.DataFrame | None:
    """Read all files matching pattern for a specific source."""
    files = list(input_dir.glob(pattern))
    
    if not files:
        return None
    
    print(f"\n📂 {source_name} ({len(files)} files)")
    
    dataframes = []
    for file in files:
        df = read_excel(file)
        if "Source" not in df.columns:
            df = df.with_columns(pl.lit(source_name).alias("Source"))
        dataframes.append(df)
    
    combined = pl.concat(dataframes, how="diagonal")
    print(f"   ✅ {len(combined):,} total rows")
    return combined


def read_source_folder(folder_path: Path, source_name: str = None) -> pl.DataFrame | None:
    """Read all Excel files from a source folder."""
    files = list(folder_path.glob("*.xlsx")) + list(folder_path.glob("*.xls"))
    
    if not files:
        return None
    
    if source_name is None:
        source_name = folder_path.name.title()
    
    print(f"\n📂 {source_name} ({len(files)} files)")
    
    dataframes = []
    for file in files:
        df = read_excel(file)
        if "Source" not in df.columns:
            df = df.with_columns(pl.lit(source_name).alias("Source"))
        dataframes.append(df)
    
    combined = pl.concat(dataframes, how="diagonal")
    print(f"   ✅ {len(combined):,} total rows")
    return combined


def extract_all_sources(config: dict = None) -> dict[str, pl.DataFrame]:
    """
    Extract data from all sources.
    
    1. First: Load sources defined in config.yaml
    2. Then: Auto-detect any subfolders not in config
    
    Config example:
        sources:
          shopee:
            pattern: "Shopee*.xlsx"
            source_name: "Shopee"
    
    Auto-detect: data/raw/website/*.xlsx → Source: "Website"
    """
    if config is None:
        config = load_config()
    
    input_dir = Path(config["paths"]["input_dir"])
    sources_config = config.get("sources", {})
    results = {}
    configured_folders = set()
    
    # Method 1: Config-based sources
    for source_key, source_conf in sources_config.items():
        pattern = source_conf.get("pattern", f"{source_key}*.xlsx")
        source_name = source_conf.get("source_name", source_key.title())
        
        # Check if pattern points to a subfolder
        if "/" in pattern:
            folder_name = pattern.split("/")[0]
            configured_folders.add(folder_name.lower())
        
        df = read_source_files(input_dir, pattern, source_name)
        if df is not None:
            results[source_name] = df
            configured_folders.add(source_key.lower())
    
    # Method 2: Auto-detect subfolders not in config
    subfolders = [f for f in input_dir.iterdir() if f.is_dir()]
    
    for folder in subfolders:
        if folder.name.lower() not in configured_folders:
            df = read_source_folder(folder)
            if df is not None:
                source_name = folder.name.title()
                results[source_name] = df
    
    # Method 3: Files in root (legacy) - group by prefix
    root_files = list(input_dir.glob("*.xlsx")) + list(input_dir.glob("*.xls"))
    
    if root_files:
        from collections import defaultdict
        grouped = defaultdict(list)
        
        for f in root_files:
            prefix = f.stem.split()[0] if f.stem.split() else f.stem
            grouped[prefix].append(f)
        
        for prefix, files in grouped.items():
            # Skip if already loaded from config
            if prefix in results or prefix.lower() in configured_folders:
                continue
                
            print(f"\n📂 {prefix} ({len(files)} files in root)")
            dataframes = []
            for file in files:
                df = read_excel(file)
                if "Source" not in df.columns:
                    df = df.with_columns(pl.lit(prefix).alias("Source"))
                dataframes.append(df)
            
            combined = pl.concat(dataframes, how="diagonal")
            print(f"   ✅ {len(combined):,} total rows")
            results[prefix] = combined
    
    print(f"\n📊 Loaded {len(results)} sources: {list(results.keys())}")
    return results
