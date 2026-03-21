"""
Distributed Data Lake reader with Spark-to-pandas conversion.
"""

from typing import Iterable, Optional

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .config import ReaderConfig, SparkConfig
from .spark_session import create_spark_session


class DataLakeReader:
    """
    Read distributed columnar data from a Data Lake using PySpark.

    The class is intentionally format-focused for Data Lake scenarios where
    Parquet is the primary exchange/storage format.
    """

    def __init__(
        self,
        spark_config: Optional[SparkConfig] = None,
        reader_config: Optional[ReaderConfig] = None,
    ) -> None:
        self.spark_config = spark_config or SparkConfig()
        self.reader_config = reader_config or ReaderConfig()
        self.spark = create_spark_session(self.spark_config)
        self.spark.conf.set(
            "spark.sql.execution.arrow.pyspark.enabled",
            str(self.reader_config.enable_arrow).lower(),
        )

    def read_parquet(
        self,
        path: str,
        columns: Optional[Iterable[str]] = None,
        where_sql: Optional[str] = None,
    ) -> DataFrame:
        """
        Read Parquet data into a Spark DataFrame.

        Args:
            path: Data Lake URI/path, for example:
                - s3a://bucket/path/to/table
                - hdfs:///warehouse/table
                - abfss://container@account.dfs.core.windows.net/table
                - local filesystem path
            columns: Optional list of selected columns.
            where_sql: Optional SQL predicate string, e.g. "event_date >= '2026-01-01'".

        Returns:
            A Spark DataFrame after projection and filtering.
        """
        df = self.spark.read.parquet(path)

        if columns:
            df = df.select(*columns)

        if where_sql:
            df = df.where(where_sql)

        return df

    def to_pandas(
        self,
        spark_df: DataFrame,
        sample_ratio: Optional[float] = None,
        random_seed: int = 42,
    ) -> pd.DataFrame:
        """
        Convert Spark DataFrame to pandas DataFrame safely.

        Args:
            spark_df: Input Spark DataFrame.
            sample_ratio: Optional ratio in (0, 1] to sample before conversion.
            random_seed: Random seed used when sampling.

        Returns:
            pandas.DataFrame collected from Spark.

        Raises:
            ValueError: If sample ratio is invalid or dataset exceeds row guard.
        """
        if sample_ratio is not None and not (0.0 < sample_ratio <= 1.0):
            raise ValueError("sample_ratio must be in (0, 1].")

        materialized_df = spark_df
        if sample_ratio is not None and sample_ratio < 1.0:
            materialized_df = spark_df.sample(withReplacement=False, fraction=sample_ratio, seed=random_seed)

        if self.reader_config.max_rows_for_pandas is not None:
            # Count once to keep a strict memory-safety boundary before collect.
            n_rows = materialized_df.select(F.count("*").alias("n")).collect()[0]["n"]
            if n_rows > self.reader_config.max_rows_for_pandas:
                raise ValueError(
                    "Refused conversion to pandas: row count "
                    f"{n_rows} exceeds max_rows_for_pandas="
                    f"{self.reader_config.max_rows_for_pandas}."
                )

        return materialized_df.toPandas()

    def read_parquet_as_pandas(
        self,
        path: str,
        columns: Optional[Iterable[str]] = None,
        where_sql: Optional[str] = None,
        sample_ratio: Optional[float] = None,
        random_seed: int = 42,
    ) -> pd.DataFrame:
        """
        Convenience method: read Parquet and directly return a pandas DataFrame.
        """
        spark_df = self.read_parquet(path=path, columns=columns, where_sql=where_sql)
        return self.to_pandas(spark_df, sample_ratio=sample_ratio, random_seed=random_seed)

    def stop(self) -> None:
        """Stop Spark session to release cluster resources."""
        self.spark.stop()
