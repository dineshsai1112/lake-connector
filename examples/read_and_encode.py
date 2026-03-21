"""
Example script: read Parquet from Data Lake and convert to pandas.
"""

from lake_connector import DataLakeReader, ReaderConfig, SparkConfig
from lake_connector.pipeline import parquet_to_encoded_pandas


def main() -> None:
    # Replace with your real cluster and storage settings.
    spark_config = SparkConfig(
        app_name="lake-connector-example",
        master="local[*]",
        extra_conf={
            # Example for S3-compatible storage:
            # "spark.hadoop.fs.s3a.endpoint": "http://127.0.0.1:9000",
            # "spark.hadoop.fs.s3a.access.key": "minioadmin",
            # "spark.hadoop.fs.s3a.secret.key": "minioadmin",
            # "spark.hadoop.fs.s3a.path.style.access": "true",
        },
    )
    reader_config = ReaderConfig(enable_arrow=True, max_rows_for_pandas=500_000)
    reader = DataLakeReader(spark_config=spark_config, reader_config=reader_config)

    try:
        pdf = parquet_to_encoded_pandas(
            reader=reader,
            parquet_path="data/sample.parquet",
            categorical_columns=["country", "device_type"],
            columns=["user_id", "country", "device_type", "event_ts"],
            where_sql="event_ts >= '2026-01-01'",
            sample_ratio=0.5,
        )
        print(pdf.head())
        print(f"Rows: {len(pdf)}, Columns: {len(pdf.columns)}")
    finally:
        reader.stop()


if __name__ == "__main__":
    main()
