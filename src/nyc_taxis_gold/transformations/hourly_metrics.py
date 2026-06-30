# Databricks notebook source
"""gold.hourly_metrics: grouped by date_time -> metrics:
>total(avg, min, max, sum)
>fare(avg, min, max, sum)
>tips(avg, min, max, sum) 
>duration (avg, min, max)
>weather(avg rain, total rain, avg temp, avg wind)
>distance(avg, min, max)"""

# COMMAND ----------

from pyspark.sql.functions import col, to_date, avg, count, sum, max, min, date_trunc
s_main_df = spark.table('`nyc_taxis_weather`.silver_nyc_taxis.s_nyc_taxis_weather_oct_dec').withColumn("date_trunced", date_trunc("hour", "pickup_datetime")).select("pickup_date","date_trunced", "total_amount", "fare_amount", "tip_amount", "trip_duration", "trip_distance", "prcp", "temp", "wspd").drop(col("_rescued_data"))

# COMMAND ----------

g_hourly_metrics = s_main_df.groupBy("date_trunced").agg(count("*").alias("count_trips"),
                         avg(col("total_amount")).alias("avg_total"),
                         min(col("total_amount")).alias("min_total"),
                         max(col("total_amount")).alias("max_total"),
                         sum(col("total_amount")).alias("total_hourly_sum"),
                         avg(col("fare_amount")).alias("avg_fare"),
                         min(col("fare_amount")).alias("min_fare"),
                         max(col("fare_amount")).alias("max_fare"),
                         sum(col("fare_amount")).alias("fare_hourly_sum"),
                         avg(col("tip_amount")).alias("avg_tip"),
                         min(col("tip_amount")).alias("min_tip"),
                         max(col("tip_amount")).alias("max_tip"),
                         sum(col("tip_amount")).alias("tip_hourly_sum"),
                         avg(col("trip_duration")).alias("avg_duration"),
                         min(col("trip_duration")).alias("min_duration"),
                         max(col("trip_duration")).alias("max_duration"),
                         avg(col("trip_distance")).alias("avg_distance"),
                         min(col("trip_distance")).alias("min_distance"),
                         max(col("trip_distance")).alias("max_distance"),
                         avg(col("prcp")).alias("avg_rain"),
                         sum(col("prcp")).alias("total_rain"),
                         avg(col("temp")).alias("avg_temp"),
                         avg(col("wspd")).alias("avg_wind")).orderBy("date_trunced")


# COMMAND ----------

g_hourly_metrics.write.option("mergeSchema", "true").option("overwriteSchema", "true").mode("overwrite").saveAsTable("`nyc_taxis_weather`.gold_nyc_taxis.g_hourly_metrics")