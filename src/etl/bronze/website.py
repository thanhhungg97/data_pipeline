"""Website-specific Bronze ETL."""

import polars as pl

from .base import BronzeETL


class WebsiteBronzeETL(BronzeETL):
    """Website-specific extraction logic (Suppersport, Columbia, Underamour)."""

    def __init__(self, config):
        super().__init__(config)
        self.file_pattern = "*.xlsx"

    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        """Website-specific bronze transform."""
        df = super().transform(df)

        # Website data might have different column names
        # Map them to standard names
        column_mapping = {
            "Order Date": "Date",
            "Order No": "Order ID",
            "Order Status": "Status",
            "Cancel Reason": "Reason cancelled",
        }

        for old_name, new_name in column_mapping.items():
            if old_name in df.columns:
                df = df.rename({old_name: new_name})

        return df
