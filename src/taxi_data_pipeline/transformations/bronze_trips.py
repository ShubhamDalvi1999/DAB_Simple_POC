from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp


# Bronze layer - Raw NYC Taxi trips data
@dp.table(
    comment="Bronze table containing raw NYC Taxi trip data"
)
def bronze_taxi_trips():
    """
    Ingests raw NYC Taxi trips from the samples catalog.
    This serves as the bronze (raw) layer in the medallion architecture.
    """
    return (
        spark.read.table("samples.nyctaxi.trips")
        .withColumn("ingestion_timestamp", current_timestamp())
    )

