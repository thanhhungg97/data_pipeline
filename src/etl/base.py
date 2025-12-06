"""Base ETL classes and utilities."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path

import polars as pl
import yaml


@dataclass
class ETLConfig:
    """Configuration for ETL pipeline."""

    source_name: str
    input_dir: Path
    bronze_dir: Path
    silver_dir: Path
    gold_dir: Path
    config: dict = field(default_factory=dict)

    @classmethod
    def from_yaml(cls, config_path: str = "config.yaml", source_name: str = "") -> "ETLConfig":
        """Load config from YAML file."""
        with open(config_path, encoding="utf-8") as f:
            config = yaml.safe_load(f)

        base_input = Path(config["paths"]["input_dir"])
        base_output = Path(config["paths"]["output_dir"]).parent

        return cls(
            source_name=source_name,
            input_dir=base_input / source_name.lower() if source_name else base_input,
            bronze_dir=base_output / "bronze" / source_name.lower()
            if source_name
            else base_output / "bronze",
            silver_dir=base_output / "silver" / source_name.lower()
            if source_name
            else base_output / "silver",
            gold_dir=base_output / "gold",
            config=config,
        )

    @property
    def status_mapping(self) -> dict[str, str]:
        """Get status mapping from config."""
        return self.config.get("status_mapping", {})

    @property
    def schema(self) -> dict[str, str]:
        """Get schema from config."""
        return self.config.get("schema", {})


class BaseETL(ABC):
    """Base class for ETL operations."""

    def __init__(self, config: ETLConfig):
        self.config = config
        self.source_name = config.source_name

    @abstractmethod
    def extract(self) -> pl.DataFrame:
        """Extract data from source."""
        pass

    @abstractmethod
    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        """Transform data."""
        pass

    @abstractmethod
    def load(self, df: pl.DataFrame, output_dir: Path) -> Path:
        """Load data to destination."""
        pass

    def run(self) -> pl.DataFrame:
        """Run the full ETL pipeline."""
        print(f"\n{'=' * 50}")
        print(f"Running ETL for: {self.source_name}")
        print("=" * 50)

        df = self.extract()
        df = self.transform(df)
        self.load(df, self.get_output_dir())

        return df

    @abstractmethod
    def get_output_dir(self) -> Path:
        """Get the output directory for this ETL layer."""
        pass


def save_partitioned(
    df: pl.DataFrame,
    output_dir: Path,
    partition_cols: list[str] = None,
) -> int:
    """Save DataFrame partitioned by columns (default: Year/Month)."""
    if partition_cols is None:
        partition_cols = ["Year", "Month"]

    # Filter out nulls in partition columns
    for col in partition_cols:
        if col in df.columns:
            df = df.filter(pl.col(col).is_not_null())

    if len(df) == 0:
        return 0

    # Get unique partitions
    unique_parts = df.select(partition_cols).unique().sort(partition_cols)
    total_saved = 0

    for row in unique_parts.iter_rows(named=True):
        # Build filter
        filter_expr = pl.lit(True)
        path_parts = []
        for col in partition_cols:
            val = row[col]
            filter_expr = filter_expr & (pl.col(col) == val)
            path_parts.append(f"{int(val):02d}" if col == "Month" else str(int(val)))

        partition_df = df.filter(filter_expr)
        partition_path = output_dir / "/".join(path_parts) / "orders.parquet"
        partition_path.parent.mkdir(parents=True, exist_ok=True)
        partition_df.write_parquet(partition_path)
        total_saved += len(partition_df)

    return total_saved
