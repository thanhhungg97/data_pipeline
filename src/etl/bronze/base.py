"""Base Bronze ETL - Raw data extraction."""

from pathlib import Path

import polars as pl

from ..base import BaseETL, ETLConfig, save_partitioned


def _parse_date_column(df: pl.DataFrame) -> pl.DataFrame:
    """Parse Date column with multiple format attempts."""
    if "Date" not in df.columns:
        return df

    # If already datetime, just return
    if df["Date"].dtype in (pl.Datetime, pl.Date):
        return df.with_columns(pl.col("Date").cast(pl.Datetime).alias("Date"))

    # Try different date formats for string columns
    date_formats = [
        "%m-%d-%y",  # 01-13-24
        "%m/%d/%y",  # 01/13/24
        "%Y-%m-%d",  # 2024-01-13
        "%d-%m-%Y",  # 13-01-2024
        "%m-%d-%Y",  # 01-13-2024
    ]

    for fmt in date_formats:
        try:
            parsed = df.with_columns(
                pl.col("Date").str.strptime(pl.Datetime, fmt, strict=False).alias("Date")
            )
            # Check if parsing worked (not all nulls)
            if parsed["Date"].null_count() < len(parsed):
                return parsed
        except Exception:
            continue

    # Fallback: try automatic casting with strict=False
    try:
        return df.with_columns(pl.col("Date").cast(pl.Datetime, strict=False).alias("Date"))
    except Exception:
        return df


class BronzeETL(BaseETL):
    """Bronze layer: Extract raw data with minimal processing."""

    def __init__(self, config: ETLConfig):
        super().__init__(config)
        self.file_pattern = "*.xlsx"

    def extract(self) -> pl.DataFrame:
        """Extract data from Excel files."""
        input_path = self.config.input_dir
        excel_files = list(input_path.glob(self.file_pattern))

        if not excel_files:
            print(f"  ⚠️  No files found in {input_path}")
            return pl.DataFrame()

        print(f"  📂 Reading from {input_path}")
        dataframes = []

        for file in sorted(excel_files):
            print(f"    → {file.name}", end="")
            df = pl.read_excel(file)
            df = df.with_columns(pl.lit(file.name).alias("_source_file"))

            # Ensure Date column is consistent Datetime type
            if "Date" in df.columns:
                df = _parse_date_column(df)

            dataframes.append(df)
            print(f" ({len(df):,} rows)")

        if not dataframes:
            return pl.DataFrame()

        combined = pl.concat(dataframes, how="diagonal")
        print(f"  ✅ Extracted {len(combined):,} total rows")
        return combined

    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        """Bronze transform: minimal processing, just ensure Date column."""
        if len(df) == 0:
            return df

        # Ensure Date is datetime
        if "Date" in df.columns:
            df = _parse_date_column(df)

        # Add Year/Month for partitioning
        if "Date" in df.columns:
            df = df.with_columns(
                [
                    pl.col("Date").dt.year().cast(pl.Int32).alias("Year"),
                    pl.col("Date").dt.month().cast(pl.Int32).alias("Month"),
                ]
            )

        # Add source name
        df = df.with_columns(pl.lit(self.source_name).alias("Source"))

        return df

    def load(self, df: pl.DataFrame, output_dir: Path) -> Path:
        """Save to Bronze layer (partitioned by Year/Month)."""
        if len(df) == 0:
            print("  ⚠️  No data to save")
            return output_dir

        print(f"  💾 Saving to {output_dir}")
        total = save_partitioned(df, output_dir)
        print(f"  ✅ Saved {total:,} rows to Bronze")
        return output_dir

    def get_output_dir(self) -> Path:
        """Bronze output directory."""
        return self.config.bronze_dir
