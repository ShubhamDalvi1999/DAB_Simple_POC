"""
Creates a Delta table from sample data.
This script creates an actual Delta table (not a DLT view) that can be read by other jobs.
"""
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp


def parse_args():
    parser = argparse.ArgumentParser(description="Create a Delta table")
    parser.add_argument("--catalog", required=True, help="Unity Catalog name")
    parser.add_argument("--schema", required=True, help="Schema name")
    parser.add_argument(
        "--table",
        default="taxi_trips_delta",
        help="Delta table name to create",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    full_table_path = f"{args.catalog}.{args.schema}.{args.table}"

    spark = SparkSession.builder.getOrCreate()

    # Read from sample data source
    print(f"Reading from samples.nyctaxi.trips...")
    source_df = spark.read.table("samples.nyctaxi.trips")
    
    # Select a subset of columns and add a timestamp
    df = source_df.select(
        col("tpep_pickup_datetime").alias("pickup_datetime"),
        col("tpep_dropoff_datetime").alias("dropoff_datetime"),
        col("trip_distance"),
        col("fare_amount"),
        col("pickup_zip"),
        col("dropoff_zip"),
    ).withColumn("created_at", current_timestamp())

    # Create or replace the Delta table
    print(f"Creating Delta table: {full_table_path}")
    df.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(full_table_path)

    # Verify the table was created
    count = spark.table(full_table_path).count()
    print(f"✓ Delta table created successfully!")
    print(f"✓ Table contains {count} rows")
    print(f"✓ Table location: {full_table_path}")


if __name__ == "__main__":
    main()

