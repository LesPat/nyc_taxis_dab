from pyspark import pipelines as dp

from nyc_taxis_dab.taxis import read_s3_parquet


@dp.table
def weather():
    return read_s3_parquet(spark.conf.get("weather_s3_path"))
