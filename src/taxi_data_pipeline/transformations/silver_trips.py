from pyspark import pipelines as dp
from pyspark.sql.functions import col, when, round


# Silver layer - Cleaned and transformed taxi data
@dp.table(
    comment="Silver table with cleaned and enriched taxi trip data"
)
def silver_taxi_trips():
    """
    Transforms bronze taxi data by:
    - Filtering out invalid records
    - Adding calculated fields (fare_per_mile)
    - Standardizing data types
    
    Available columns from source: fare_amount, pickup_zip, dropoff_zip, trip_distance
    """
    return (
        spark.read.table("bronze_taxi_trips")
        .filter(col("fare_amount") > 0)
        .filter(col("trip_distance") > 0)
        .withColumn("fare_per_mile", 
                    when(col("trip_distance") > 0, 
                         round(col("fare_amount") / col("trip_distance"), 2))
                    .otherwise(0))
    )

