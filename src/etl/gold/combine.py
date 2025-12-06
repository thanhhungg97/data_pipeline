"""Gold ETL - Combine all sources and create aggregations."""

from pathlib import Path

import polars as pl

from ..base import BaseETL, ETLConfig, save_partitioned


class GoldETL(BaseETL):
    """Gold layer: Combine all Silver data and create aggregations."""

    def __init__(self, config: ETLConfig, silver_sources: list[str] = None):
        super().__init__(config)
        self.silver_sources = silver_sources or []
        self.source_name = "all_sources"

    def extract(self) -> pl.DataFrame:
        """Extract data from all Silver layers."""
        # Silver base is data/silver/
        silver_base = Path("data/silver")

        all_dfs = []
        print("  📂 Combining Silver data from all sources")

        for source in self.silver_sources:
            source_dir = silver_base / source.lower()
            parquet_files = list(source_dir.glob("**/*.parquet"))

            if parquet_files:
                df = pl.concat([pl.read_parquet(f) for f in parquet_files], how="diagonal")
                print(f"    → {source}: {len(df):,} rows")
                all_dfs.append(df)
            else:
                print(f"    → {source}: No data found")

        if not all_dfs:
            print("  ⚠️  No Silver data found")
            return pl.DataFrame()

        combined = pl.concat(all_dfs, how="diagonal")
        print(f"  ✅ Combined {len(combined):,} total rows")
        return combined

    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        """Gold transform: ensure consistency across sources."""
        if len(df) == 0:
            return df

        # Ensure all required columns exist
        required_cols = [
            "Date",
            "Source",
            "Order ID",
            "Status",
            "Status_Normalized",
            "Year",
            "Month",
        ]
        for col in required_cols:
            if col not in df.columns:
                df = df.with_columns(pl.lit(None).alias(col))

        # Sort by date
        if "Date" in df.columns:
            df = df.sort("Date")

        print(f"  ✅ Gold transform complete: {len(df):,} rows")
        return df

    def load(self, df: pl.DataFrame, output_dir: Path) -> Path:
        """Save to Gold layer."""
        if len(df) == 0:
            print("  ⚠️  No data to save")
            return output_dir

        # Save combined data (partitioned)
        combined_dir = output_dir / "all_sources"
        print(f"  💾 Saving combined data to {combined_dir}")
        total = save_partitioned(df, combined_dir)
        print(f"  ✅ Saved {total:,} rows to Gold/all_sources")

        # Also create metrics aggregations
        self._create_metrics(df, output_dir)

        return output_dir

    def _create_metrics(self, df: pl.DataFrame, output_dir: Path):
        """Create aggregated metrics for dashboard."""
        metrics_dir = output_dir / "metrics"
        metrics_dir.mkdir(parents=True, exist_ok=True)

        # Monthly metrics by source
        status_col = "Status_Normalized" if "Status_Normalized" in df.columns else "Status"

        monthly_metrics = (
            df.group_by(["Source", "Year", "Month"])
            .agg(
                [
                    pl.len().alias("total_orders"),
                    pl.col(status_col)
                    .filter(pl.col(status_col) == "Delivered")
                    .len()
                    .alias("delivered"),
                    pl.col(status_col)
                    .filter(pl.col(status_col) == "Cancelled")
                    .len()
                    .alias("cancelled"),
                    pl.col(status_col)
                    .filter(pl.col(status_col) == "Returned")
                    .len()
                    .alias("returned"),
                    pl.col(status_col).filter(pl.col(status_col) == "Failed").len().alias("failed"),
                ]
            )
            .sort(["Source", "Year", "Month"])
        )

        # Add rates
        monthly_metrics = monthly_metrics.with_columns(
            [
                (pl.col("delivered") / pl.col("total_orders") * 100)
                .round(1)
                .alias("delivery_rate"),
                (pl.col("cancelled") / pl.col("total_orders") * 100).round(1).alias("cancel_rate"),
            ]
        )

        monthly_metrics.write_parquet(metrics_dir / "monthly_by_source.parquet")
        print(f"  ✅ Saved monthly metrics ({len(monthly_metrics)} records)")

        # Cancellation reasons
        if "Reason cancelled" in df.columns:
            reasons = (
                df.filter(pl.col(status_col) == "Cancelled")
                .filter(pl.col("Reason cancelled").is_not_null())
                .group_by(["Year", "Month", "Reason cancelled"])
                .agg(pl.len().alias("count"))
                .sort(["Year", "Month", "count"], descending=[False, False, True])
            )
            reasons.write_parquet(metrics_dir / "cancellation_reasons.parquet")
            print(f"  ✅ Saved cancellation reasons ({len(reasons)} records)")

    def get_output_dir(self) -> Path:
        """Gold output directory."""
        return self.config.gold_dir
