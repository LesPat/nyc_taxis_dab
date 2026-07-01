from pyspark import pipelines as dp

from nyc_taxis_dab.taxis import cast_timestamp_ntz_columns, read_s3_mixed


@dp.table
def weather():
    return cast_timestamp_ntz_columns(read_s3_mixed(spark.conf.get("weather_s3_path")))
