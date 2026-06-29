from pyspark import pipelines as dp
from pyspark.sql.functions import col, hour, to_date


@dp.table
def taxi_with_weather():
    pickup_col = spark.conf.get("taxi_pickup_column")
    trips = (
        spark.read.table("taxi_fares")
        .withColumn("pickup_date", to_date(col(pickup_col)))
        .withColumn("pickup_hour", hour(col(pickup_col)))
    )
    conditions = spark.read.table("weather_hourly")

    return trips.join(
        conditions,
        (col("pickup_date") == col("weather_date"))
        & (col("pickup_hour") == col("weather_hour")),
        how="left",
    )
