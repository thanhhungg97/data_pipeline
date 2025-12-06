"""Base Silver ETL - Data cleaning and normalization."""

from pathlib import Path

import polars as pl

from ..base import BaseETL, ETLConfig, save_partitioned


class SilverETL(BaseETL):
    """Silver layer: Clean, normalize, and validate data."""

    def __init__(self, config: ETLConfig):
        super().__init__(config)

    def extract(self) -> pl.DataFrame:
        """Extract data from Bronze layer."""
        bronze_dir = self.config.bronze_dir
        parquet_files = list(bronze_dir.glob("**/*.parquet"))

        if not parquet_files:
            print(f"  ⚠️  No Bronze data found in {bronze_dir}")
            return pl.DataFrame()

        print(f"  📂 Reading from Bronze: {bronze_dir}")
        df = pl.concat([pl.read_parquet(f) for f in parquet_files], how="diagonal")
        print(f"  ✅ Loaded {len(df):,} rows from Bronze")
        return df

    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        """Silver transform: normalize, clean, validate."""
        if len(df) == 0:
            return df

        # 1. Normalize Status
        df = self._normalize_status(df)

        # 2. Clean strings
        df = self._clean_strings(df)

        # 3. Validate and filter
        df = self._validate(df)

        # 4. Select standard columns
        df = self._select_standard_columns(df)

        print(f"  ✅ Transformed {len(df):,} rows")
        return df

    def _normalize_status(self, df: pl.DataFrame) -> pl.DataFrame:
        """Normalize status values using config mapping."""
        if "Status" not in df.columns:
            return df

        status_mapping = self.config.status_mapping

        df = df.with_columns(
            pl.col("Status")
            .map_elements(
                lambda x: status_mapping.get(x, x) if x else None,
                return_dtype=pl.Utf8,
            )
            .alias("Status_Normalized")
        )
        return df

    def _clean_strings(self, df: pl.DataFrame) -> pl.DataFrame:
        """Strip whitespace from string columns."""
        string_cols = [col for col, dtype in zip(df.columns, df.dtypes) if dtype == pl.Utf8]
        if string_cols:
            df = df.with_columns([pl.col(col).str.strip_chars() for col in string_cols])
        return df

    def _validate(self, df: pl.DataFrame) -> pl.DataFrame:
        """Validate data - remove invalid rows."""
        # Remove rows with null dates
        if "Date" in df.columns:
            df = df.filter(pl.col("Date").is_not_null())

        # Remove rows where Year/Month is null
        if "Year" in df.columns:
            df = df.filter(pl.col("Year").is_not_null())

        return df

    def _select_standard_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        """Select and order standard columns."""
        standard_cols = [
            "Date",
            "Source",
            "Order ID",
            "Status",
            "Status_Normalized",
            "Reason cancelled",
            "Year",
            "Month",
        ]

        # Only select columns that exist
        available_cols = [col for col in standard_cols if col in df.columns]

        # Add any extra columns not in standard list
        extra_cols = [
            col for col in df.columns if col not in standard_cols and not col.startswith("_")
        ]

        return df.select(available_cols + extra_cols)

    def load(self, df: pl.DataFrame, output_dir: Path) -> Path:
        """Save to Silver layer (partitioned by Year/Month)."""
        if len(df) == 0:
            print("  ⚠️  No data to save")
            return output_dir

        print(f"  💾 Saving to {output_dir}")
        total = save_partitioned(df, output_dir)
        print(f"  ✅ Saved {total:,} rows to Silver")
        return output_dir

    def get_output_dir(self) -> Path:
        """Silver output directory."""
        return self.config.silver_dir
