"""
Simple job to read a Delta table and print a small preview.
Runs as a spark_python_task (no notebook needed).
"""
import argparse
from pyspark.sql import SparkSession


def parse_args():
    parser = argparse.ArgumentParser(description="Preview a Delta table")
    parser.add_argument("--catalog", required=True, help="Unity Catalog name")
    parser.add_argument("--schema", required=True, help="Schema name")
    parser.add_argument(
        "--table",
        default="taxi_trips_delta",
        help="Delta table name within the catalog.schema",
    )
    parser.add_argument("--limit", type=int, default=5, help="Rows to display")
    return parser.parse_args()


def main():
    args = parse_args()
    full_table = f"{args.catalog}.{args.schema}.{args.table}"

    spark = SparkSession.builder.getOrCreate()

    df = spark.table(full_table)
    row_count = df.count()

    print(f"Table: {full_table}")
    print(f"Row count: {row_count}")
    print("Schema:")
    df.printSchema()

    print(f"Top {args.limit} rows:")
    df.show(args.limit, truncate=False)


if __name__ == "__main__":
    main()

