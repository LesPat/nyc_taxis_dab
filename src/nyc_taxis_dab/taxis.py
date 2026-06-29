from functools import reduce

from databricks.sdk.runtime import spark
from pyspark.sql import DataFrame


def read_s3_parquet(path: str) -> DataFrame:
    return spark.read.parquet(path)


def read_s3_csv(path: str) -> DataFrame:
    return spark.read.option("header", "true").option("inferSchema", "true").csv(path)


def read_s3_mixed(path: str) -> DataFrame:
    """Read a directory that contains both Parquet and CSV files."""
    frames: list[DataFrame] = []

    try:
        frames.append(read_s3_parquet(path))
    except Exception:
        pass

    try:
        frames.append(read_s3_csv(path))
    except Exception:
        pass

    if not frames:
        raise ValueError(f"No readable Parquet or CSV files found at {path}")

    return reduce(lambda left, right: left.unionByName(right, allowMissingColumns=True), frames)


def read_s3_path(path: str, data_format: str | None = None) -> DataFrame:
    fmt = data_format or spark.conf.get("data_format", "parquet")
    if fmt == "mixed":
        return read_s3_mixed(path)
    if fmt == "csv":
        return read_s3_csv(path)
    return read_s3_parquet(path)


def load_taxi_fares(path: str | None = None) -> DataFrame:
    """Read raw taxi fares from S3 (Parquet + optional CSV in the same folder)."""
    s3_path = path or spark.conf.get("taxi_s3_path")
    return read_s3_mixed(s3_path)
