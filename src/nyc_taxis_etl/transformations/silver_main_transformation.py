"""
Silver-layer transformation for NYC taxi ETL.

Pipeline flow (upstream → this file → output table):

  taxi_fares (S3)  ──┐
  weather (S3)     ──┼──► nyc_taxis_silver  (partitioned by pickup_date)
  zone lookup (UC) ──┤
  weather codes (UC)┘

Each @dp.table in transformations/ is a step in the Lakeflow pipeline.
This module is the final curated table used for analysis.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, date_trunc, round, to_date

from nyc_taxis_dab.mappings import SILVER_TAXI_COLUMN_MAP


def _drop_columns_if_present(df: DataFrame, *columns: str) -> DataFrame:
    """Drop join/helper columns only when they exist (schemas can differ by source file)."""
    existing = [name for name in columns if name in df.columns]
    return df.drop(*existing) if existing else df


def _pickup_zone_lookup(lookup_df: DataFrame) -> DataFrame:
    """Prepare zone lookup for pickup location join (PULocationID → borough/zone names)."""
    return (
        lookup_df.withColumnRenamed("LocationID", "PULocationID_tmp")
        .withColumnRenamed("Borough", "PUBorough")
        .withColumnRenamed("Zone", "PUZone")
        .drop("service_zone")
    )


def _dropoff_zone_lookup(lookup_df: DataFrame) -> DataFrame:
    """Prepare zone lookup for dropoff location join (DOLocationID → borough/zone names)."""
    return (
        lookup_df.withColumnRenamed("LocationID", "DOLocationID_tmp")
        .withColumnRenamed("Borough", "DOBorough")
        .withColumnRenamed("Zone", "DOZone")
        .drop("service_zone")
    )


@dp.table(name="nyc_taxis_silver", partition_cols=["pickup_date"])
def silver_main_transformation():
    # --- 1. Load inputs ---
    # taxi_fares / weather: bronze tables from earlier pipeline steps (S3 reads).
    # lookup tables: Unity Catalog reference data (values in databricks.yml → pipeline configuration):
    #   taxi_zone_lookup_table → nyc_taxis_weather.bronze_nyc_taxis.b_taxi_zone_lookup
    #   weather_codes_table    → nyc_taxis_weather.bronze_nyc_taxis.b_weather_codes
    taxis_df = spark.read.table("taxi_fares")
    weather_df = spark.read.table("weather")
    lookup_df = spark.read.table(spark.conf.get("taxi_zone_lookup_table"))
    w_code_df = spark.read.table(spark.conf.get("weather_codes_table"))

    weather_time_col = spark.conf.get("weather_time_column")

    # --- 2. Join weather (hourly) ---
    # Weather is one row per hour, so truncate pickup timestamp to the hour
    # and match it to the weather `time` column.
    dt_taxis_hour = taxis_df.withColumn(
        "pickup_hour",
        date_trunc("hour", col("tpep_pickup_datetime")),
    )

    dt_taxis_weather = dt_taxis_hour.join(
        weather_df,
        col("pickup_hour") == col(weather_time_col),
        how="left",
    )
    # Remove join keys and duplicate weather timestamp columns.
    dt_taxis_weather = _drop_columns_if_present(
        dt_taxis_weather,
        "datetime",
        weather_time_col,
        "pickup_hour",
    )

    # --- 3. Enrich with zone names (pickup + dropoff) ---
    # PULocationID / DOLocationID are numeric codes; lookup adds human-readable borough/zone.
    pu_lookup = _pickup_zone_lookup(lookup_df)
    do_lookup = _dropoff_zone_lookup(lookup_df)

    result = (
        dt_taxis_weather.join(
            pu_lookup,
            col("PULocationID") == col("PULocationID_tmp"),
            how="left",
        )
        .join(
            do_lookup,
            col("DOLocationID") == col("DOLocationID_tmp"),
            how="left",
        )
        # --- 4. Decode weather condition code (coco → readable label) ---
        .join(w_code_df, col("coco") == col("w_code"), how="left")
    )

    # --- 5. Derived metrics (before column rename) ---
    # trip_duration uses original TLC column names still present at this stage.
    result = result.withColumn(
        "trip_duration",
        round(
            (col("tpep_dropoff_datetime").cast("long") - col("tpep_pickup_datetime").cast("long")) / 60,
            2,
        ),
    )

    # --- 6. Cleanup ---
    # Drop IDs, join keys, and columns not needed in the silver table.
    result = _drop_columns_if_present(
        result,
        "PULocationID_tmp",
        "DOLocationID_tmp",
        "w_code",
        "coco",
        "store_and_fwd_flag",
        "PULocationID",
        "DOLocationID",
        "congestion_surcharge",
    )

    # --- 7. Standardize names + partition column ---
    # Renames are centralized in mappings.py (SILVER_TAXI_COLUMN_MAP).
    # pickup_date drives table partitioning for efficient date-filtered queries.
    result = result.withColumnsRenamed(SILVER_TAXI_COLUMN_MAP)
    return result.withColumn("pickup_date", to_date(col("pickup_datetime")))
