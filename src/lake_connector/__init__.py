"""
lake_connector package.

This package provides:
1. A Data Lake reader based on PySpark that supports distributed columnar
   storage (for example, Parquet files on HDFS/S3/ABFS).
2. A distributed feature encoder for common machine learning preprocessing.
"""

from .config import ReaderConfig, SparkConfig
from .distributed_encoder import DistributedFeatureEncoder
from .reader import DataLakeReader

__all__ = [
    "DataLakeReader",
    "DistributedFeatureEncoder",
    "ReaderConfig",
    "SparkConfig",
]
