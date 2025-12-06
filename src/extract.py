"""Extract: Read data from Excel files."""
import polars as pl
from pathlib import Path


def read_excel(file_path: str | Path, sheet_name: str = None) -> pl.DataFrame:
    """Read an Excel file into a Polars DataFrame."""
    # Polars uses pandas under the hood for Excel, then converts
    df = pl.read_excel(file_path, sheet_name=sheet_name)
    print(f"Extracted {len(df):,} rows from {file_path}")
    return df


def read_all_excel_files(input_dir: str | Path) -> dict[str, pl.DataFrame]:
    """Read all Excel files from a directory."""
    input_path = Path(input_dir)
    dataframes = {}
    
    for file in input_path.glob("*.xlsx"):
        name = file.stem  # filename without extension
        dataframes[name] = read_excel(file)
    
    for file in input_path.glob("*.xls"):
        name = file.stem
        dataframes[name] = read_excel(file)
    
    print(f"Loaded {len(dataframes)} files: {list(dataframes.keys())}")
    return dataframes

