from pyspark import pipelines as dp
from pyspark.sql.functions import col, sum, avg, count, round


# Gold layer - Aggregated metrics by pickup zone
@dp.table(
    comment="Gold table with aggregated taxi metrics by pickup zone"
)
def gold_zone_metrics():
    """
    Creates business-level aggregations:
    - Trip counts per zone
    - Revenue metrics per zone
    - Average fare statistics
    """
    return (
        spark.read.table("silver_taxi_trips")
        .groupBy(col("pickup_zip"))
        .agg(
            count("*").alias("total_trips"),
            round(sum("fare_amount"), 2).alias("total_revenue"),
            round(avg("fare_amount"), 2).alias("avg_fare"),
            round(avg("fare_per_mile"), 2).alias("avg_fare_per_mile"),
            round(avg("trip_distance"), 2).alias("avg_trip_distance")
        )
        .orderBy(col("total_revenue").desc())
    )

