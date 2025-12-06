"""Shopee-specific Silver ETL."""

import polars as pl

from .base import SilverETL


class ShopeeSilverETL(SilverETL):
    """Shopee-specific transformation logic."""

    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        """Shopee-specific silver transform."""
        if len(df) == 0:
            return df

        # Shopee-specific business rules
        # Example: Handle specific Shopee status values
        # df = self._handle_shopee_statuses(df)

        # Apply base transform
        df = super().transform(df)

        return df

    def _validate(self, df: pl.DataFrame) -> pl.DataFrame:
        """Shopee-specific validation."""
        df = super()._validate(df)

        # Shopee-specific: Remove test orders
        if "Order ID" in df.columns:
            df = df.filter(~pl.col("Order ID").str.starts_with("TEST"))

        return df
