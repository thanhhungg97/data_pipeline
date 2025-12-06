"""Shopee-specific Bronze ETL."""

import polars as pl

from .base import BronzeETL


class ShopeeBronzeETL(BronzeETL):
    """Shopee-specific extraction logic."""

    def __init__(self, config):
        super().__init__(config)
        self.file_pattern = "*.xlsx"  # Shopee*.xlsx

    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        """Shopee-specific bronze transform."""
        df = super().transform(df)

        # Shopee-specific: rename columns if needed
        column_mapping = {
            "Order ID": "Order ID",
            "Date": "Date",
            "Status": "Status",
            "Reason cancelled": "Reason cancelled",
        }

        # Only rename if columns exist
        for old_name, new_name in column_mapping.items():
            if old_name in df.columns and old_name != new_name:
                df = df.rename({old_name: new_name})

        return df
