"""
High-level pipeline utilities for reading and distributed encoding.
"""

from typing import Iterable, Optional

import pandas as pd

from .distributed_encoder import DistributedFeatureEncoder
from .reader import DataLakeReader


def parquet_to_encoded_pandas(
    reader: DataLakeReader,
    parquet_path: str,
    categorical_columns: Iterable[str],
    columns: Optional[Iterable[str]] = None,
    where_sql: Optional[str] = None,
    sample_ratio: Optional[float] = None,
) -> pd.DataFrame:
    """
    End-to-end utility:
      1. Read Parquet from Data Lake
      2. Encode categorical columns in distributed Spark
      3. Convert final result to pandas
    """
    spark_df = reader.read_parquet(path=parquet_path, columns=columns, where_sql=where_sql)
    encoder = DistributedFeatureEncoder(categorical_columns=categorical_columns)
    transformed = encoder.fit_transform(spark_df)
    return reader.to_pandas(transformed, sample_ratio=sample_ratio)
