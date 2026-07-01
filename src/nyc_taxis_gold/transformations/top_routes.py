"""top_routes: grouped by top routes (sorted by count) -> metrics:
 >total(avg, min, max, sum)
 >fare(avg, min, max, sum)
 >tips(avg, min, max, sum)
 >duration (avg, min, max)
 >profitability (total/duration)"""

from pyspark import pipelines as dp
from pyspark.sql.functions import avg, col, concat_ws, count, max, min, sum, when

from nyc_taxis_dab.taxis import read_silver_table


@dp.table(name="top_routes")
def top_routes():
    silver = read_silver_table()
    return (
        silver.withColumn(
            "route",
            concat_ws(" > ", col("pickup_location"), col("dropoff_location")),
        )
        .groupBy("route")
        .agg(
            count("*").alias("count_trips"),
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
            (
                when(
                    sum(col("trip_duration")) != 0,
                    sum(col("total_amount")) / sum(col("trip_duration")),
                ).alias("profitability")
            ),
        )
    )
