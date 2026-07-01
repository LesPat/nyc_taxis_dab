from functools import reduce

from databricks.sdk.runtime import spark
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import TimestampNTZType, TimestampType


def _s3_glob(path: str, extension: str) -> str:
    """Build a glob path so CSV reader does not pick up Parquet binaries in the same folder."""
    return f"{path.rstrip('/')}/*.{extension}"


def read_s3_parquet(path: str) -> DataFrame:
    return spark.read.parquet(_s3_glob(path, "parquet"))


def read_s3_csv(path: str) -> DataFrame:
    return spark.read.option("header", "true").option("inferSchema", "true").csv(
        _s3_glob(path, "csv")
    )


def read_s3_mixed(path: str) -> DataFrame:
    """Read Parquet (*.parquet) and CSV (*.csv) files from the same S3 folder."""
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


def cast_timestamp_ntz_columns(df: DataFrame) -> DataFrame:
    """Cast TIMESTAMP_NTZ to TIMESTAMP so Delta tables do not need the timestampNtz feature."""
    for field in df.schema.fields:
        if isinstance(field.dataType, TimestampNTZType):
            df = df.withColumn(field.name, col(field.name).cast(TimestampType()))
    return df


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

def read_silver_table(table_name: str = "nyc_taxis_silver") -> DataFrame:
    catalog = spark.conf.get("catalog")
    silver_schema = spark.conf.get("silver_schema")
    return spark.read.table(f"{catalog}.{silver_schema}.{table_name}")