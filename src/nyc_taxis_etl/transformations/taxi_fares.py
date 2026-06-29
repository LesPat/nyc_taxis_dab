from pyspark import pipelines as dp

from nyc_taxis_dab.taxis import read_s3_mixed


@dp.table
def taxi_fares():
    return read_s3_mixed(spark.conf.get("taxi_s3_path"))
