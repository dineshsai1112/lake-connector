"""
Spark session utilities.
"""

from pyspark.sql import SparkSession

from .config import SparkConfig


def create_spark_session(config: SparkConfig) -> SparkSession:
    """
    Build a SparkSession from a typed configuration object.

    Args:
        config: Spark runtime configuration.

    Returns:
        An initialized SparkSession.
    """
    builder = SparkSession.builder.appName(config.app_name)

    if config.master:
        builder = builder.master(config.master)

    for key, value in config.extra_conf.items():
        builder = builder.config(key, value)

    return builder.getOrCreate()
