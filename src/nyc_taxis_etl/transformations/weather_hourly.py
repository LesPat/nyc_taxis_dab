from pyspark import pipelines as dp
from pyspark.sql.functions import col, hour, to_date


def _weather_date_hour_columns():
    timestamp_col = spark.conf.get("weather_datetime_column")
    return (
        to_date(col(timestamp_col)).alias("weather_date"),
        hour(col(timestamp_col)).alias("weather_hour"),
    )


@dp.table
def weather_hourly():
    weather_date, weather_hour = _weather_date_hour_columns()
    return spark.read.table("weather").select(
        "*",
        weather_date,
        weather_hour,
    )
