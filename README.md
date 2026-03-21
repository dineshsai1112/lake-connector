# lake-connector

A PySpark-based connector for machine learning workflows that read distributed
columnar Data Lake files (for example, Parquet) and convert them into
`pandas.DataFrame`.

## Design Goals

- Support distributed storage URIs, such as `s3a://`, `hdfs://`, and `abfss://`.
- Use Spark for scalable read/transform operations.
- Provide safe Spark-to-pandas conversion with explicit row guards.
- Include distributed categorical encoding with Spark ML.
- Keep package structure clean and reusable for open-source collaboration.

## Project Structure

```text
lake-connector/
  examples/
    read_and_encode.py
  src/
    lake_connector/
      __init__.py
      config.py
      spark_session.py
      reader.py
      distributed_encoder.py
      pipeline.py
  pyproject.toml
```

## Installation

```bash
pip install -e .
```

## Quick Start

```python
from lake_connector import DataLakeReader, SparkConfig, ReaderConfig

spark_cfg = SparkConfig(
    app_name="lake-reader",
    master="local[*]",
)
reader_cfg = ReaderConfig(enable_arrow=True, max_rows_for_pandas=1_000_000)
reader = DataLakeReader(spark_config=spark_cfg, reader_config=reader_cfg)

try:
    pdf = reader.read_parquet_as_pandas(
        path="s3a://my-bucket/warehouse/events/",
        columns=["user_id", "event_type", "event_ts"],
        where_sql="event_ts >= '2026-01-01'",
        sample_ratio=0.2,
    )
    print(pdf.head())
finally:
    reader.stop()
```

## Distributed Encoding

Use `DistributedFeatureEncoder` to encode categorical features in Spark:

```python
from lake_connector.distributed_encoder import DistributedFeatureEncoder

spark_df = reader.read_parquet("hdfs:///warehouse/events")
encoder = DistributedFeatureEncoder(categorical_columns=["country", "device_type"])
encoded_df = encoder.fit_transform(spark_df)
pdf = reader.to_pandas(encoded_df, sample_ratio=0.1)
```

## Open-Source Notes

- Code includes English docstrings and comments for cross-team collaboration.
- Keep data access credentials outside source code (environment variables or secret managers).
- Add tests and CI checks before production deployment.
