# lake-connector

`lake-connector` is a PySpark-based toolkit for machine learning data access in
distributed Data Lake environments. It focuses on reading columnar files (for
example, Parquet), applying distributed preprocessing, and converting curated
results into `pandas.DataFrame` for downstream analytics and modeling.

## Abstract

Large-scale feature pipelines often require both distributed data processing and
local model experimentation. This project provides a practical bridge: Spark is
used for scalable I/O and transformation, while pandas is used for local ML
workflows. The implementation introduces explicit memory-safety controls before
collecting distributed data to local runtime.

## Key Features

- Distributed Parquet reading with Spark (`s3a://`, `hdfs://`, `abfss://`, and local paths).
- Column projection and SQL-style filtering for efficient data reduction.
- Guarded Spark-to-pandas conversion with configurable row limits.
- Optional sampling before collection to support exploratory analysis.
- Distributed categorical encoding via Spark ML (`StringIndexer` + `OneHotEncoder`).

## Methodological Notes

- **Processing model**: Execute I/O and feature encoding in Spark executors.
- **Collection policy**: Convert to pandas only after row-bound validation.
- **Reproducibility**: Sampling supports deterministic behavior with a fixed seed.
- **Extensibility**: Core API is modular (`reader`, `encoder`, and `pipeline` layers).

## Requirements

- Python `>=3.10`
- Apache Spark / PySpark `>=3.5.0`
- pandas `>=2.0.0`

## Installation and Setup

```bash
pip install -e .
```

Optional: configure object storage and Spark runtime through `SparkConfig.extra_conf`.

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

## Distributed Encoding Example

Use `DistributedFeatureEncoder` to encode categorical features in Spark:

```python
from lake_connector.distributed_encoder import DistributedFeatureEncoder

spark_df = reader.read_parquet("hdfs:///warehouse/events")
encoder = DistributedFeatureEncoder(categorical_columns=["country", "device_type"])
encoded_df = encoder.fit_transform(spark_df)
pdf = reader.to_pandas(encoded_df, sample_ratio=0.1)
```

## Reproducibility and Data Governance

- Keep credentials outside source code (environment variables or secret managers).
- Record storage URI, Spark configuration, and sampling parameters in experiments.
- Use schema validation and row-level sanity checks before model training.

## Open-Source Compliance

- License: MIT (see `LICENSE`).
- Source code follows typed, modular Python package conventions.
- English docstrings/comments are used for international collaboration.
- Contributions should include tests and clear rationale in pull requests.

## Citation (Recommended)

If this project contributes to academic or industrial reports, cite the
repository and include the commit hash used in experiments for traceability.
