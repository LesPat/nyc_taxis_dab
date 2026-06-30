"""gold.daily_metrics: grouped by date -> metrics:
>total(avg, min, max, sum)
>fare(avg, min, max, sum)
>tips(avg, min, max, sum) 
>duration (avg, min, max)
>weather(avg rain, total rain, avg temp, avg wind)
>distance(avg, min, max)
"""

from pyspark.sql.functions import col, avg, count, sum, max, min
from pyspark import pipelines as dp
from nyc_taxis_dab.taxis import read_silver_table 

@dp.table(name="daily_metrics")
def daily_metrics():
    silver = read_silver_table()
    return (
        silver.groupBy("pickup_date").agg(count("*").alias("count_trips"),
                         avg(col("total_amount")).alias("avg_total"),
                         min(col("total_amount")).alias("min_total"),
                         max(col("total_amount")).alias("max_total"),
                         sum(col("total_amount")).alias("total_daily_sum"),
                         avg(col("fare_amount")).alias("avg_fare"),
                         min(col("fare_amount")).alias("min_fare"),
                         max(col("fare_amount")).alias("max_fare"),
                         sum(col("fare_amount")).alias("fare_daily_sum"),
                         avg(col("tip_amount")).alias("avg_tip"),
                         min(col("tip_amount")).alias("min_tip"),
                         max(col("tip_amount")).alias("max_tip"),
                         sum(col("tip_amount")).alias("tip_daily_sum"),
                         avg(col("trip_duration")).alias("avg_duration"),
                         min(col("trip_duration")).alias("min_duration"),
                         max(col("trip_duration")).alias("max_duration"),
                         avg(col("trip_distance")).alias("avg_distance"),
                         min(col("trip_distance")).alias("min_distance"),
                         max(col("trip_distance")).alias("max_distance"),
                         avg(col("prcp")).alias("avg_rain"),
                         sum(col("prcp")).alias("total_rain"),
                         avg(col("temp")).alias("avg_temp"),
                         avg(col("wspd")).alias("avg_wind"))
    )