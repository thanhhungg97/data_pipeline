"""Website-specific Silver ETL."""

import polars as pl

from .base import SilverETL


class WebsiteSilverETL(SilverETL):
    """Website-specific transformation logic."""

    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        """Website-specific silver transform."""
        if len(df) == 0:
            return df

        # Website-specific business rules can be added here

        # Apply base transform
        df = super().transform(df)

        return df

    def _validate(self, df: pl.DataFrame) -> pl.DataFrame:
        """Website-specific validation."""
        df = super()._validate(df)

        # Website-specific validation rules can be added here

        return df
