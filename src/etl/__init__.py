"""Multi-layer ETL Architecture.

Layers:
- Bronze: Raw data extraction (source-specific)
- Silver: Cleaned & normalized data (source-specific)
- Gold: Aggregated & combined data (cross-source)
"""

from .base import BaseETL, ETLConfig
from .pipeline import run_pipeline, run_source_pipeline

__all__ = ["BaseETL", "ETLConfig", "run_pipeline", "run_source_pipeline"]
