"""
Configuration objects for the Data Lake connector.
"""

from dataclasses import dataclass, field
from typing import Dict, Optional


@dataclass(frozen=True)
class SparkConfig:
    """
    Spark runtime configuration.

    Attributes:
        app_name: Name shown in Spark UI and logs.
        master: Spark master URL. Keep None to rely on environment defaults.
        extra_conf: Additional Spark key-value configuration.
    """

    app_name: str = "lake-connector"
    master: Optional[str] = None
    extra_conf: Dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class ReaderConfig:
    """
    Reader-level configuration for memory-safe conversion to pandas.

    Attributes:
        enable_arrow: Whether to enable Apache Arrow for faster conversion.
        max_rows_for_pandas: Hard guard to avoid collecting overly large data.
            Set to None to disable the row-count guard.
    """

    enable_arrow: bool = True
    max_rows_for_pandas: Optional[int] = 2_000_000
