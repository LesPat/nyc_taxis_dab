# Databricks notebook source
"""gold.top_routes: grouped by top routes (sorted by count) -> metrics:
 >total(avg, min, max, sum)
 >fare(avg, min, max, sum)
 >tips(avg, min, max, sum) 
 >duration (avg, min, max)
 >profitability (total/duration)"""

# COMMAND ----------

from pyspark.sql.functions import col, to_date, avg, count, sum, max, min, date_trunc, concat_ws, lit, desc
s_main_df = spark.table('`nyc_taxis_weather`.silver_nyc_taxis.s_nyc_taxis_weather_oct_dec').select("total_amount", "fare_amount", "tip_amount", "trip_duration", "trip_distance", "pickup_location", "dropoff_location", "pickup_zone", "dropoff_zone").drop(col("_rescued_data"))

# COMMAND ----------

g_top_routes = s_main_df.withColumn("route", concat_ws(" > ", "pickup_location", "dropoff_location")).groupBy("route").agg(count("*").alias("count_trips"),
                         avg(col("total_amount")).alias("avg_total"),
                         min(col("total_amount")).alias("min_total"),
                         max(col("total_amount")).alias("max_total"),
                         sum(col("total_amount")).alias("total_route_sum"),
                         avg(col("fare_amount")).alias("avg_fare"),
                         min(col("fare_amount")).alias("min_fare"),
                         max(col("fare_amount")).alias("max_fare"),
                         sum(col("fare_amount")).alias("fare_route_sum"),
                         avg(col("tip_amount")).alias("avg_tip"),
                         min(col("tip_amount")).alias("min_tip"),
                         max(col("tip_amount")).alias("max_tip"),
                         sum(col("tip_amount")).alias("tip_route_sum"),
                         avg(col("trip_duration")).alias("avg_duration"),
                         min(col("trip_duration")).alias("min_duration"),
                         max(col("trip_duration")).alias("max_duration"),
                         avg(col("trip_distance")).alias("avg_distance"),
                         min(col("trip_distance")).alias("min_distance"),
                         max(col("trip_distance")).alias("max_distance"),
                         (sum(col("total_amount"))/sum(col("trip_duration"))).alias("profitability"))\
                             .select("*", lit("NYC").alias("city"))\
                                 .sort("count_trips", ascending=False)
                         

# COMMAND ----------

g_top_routes.write.option("mergeSchema", "true").option("overwriteSchema", "true").mode("overwrite").saveAsTable("`nyc_taxis_weather`.gold_nyc_taxis.g_top_routes")