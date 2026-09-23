"""
Read raw CSV into a typed Bronze Delta table.
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType


SCHEMA = StructType(
    [
        StructField("trip_id", StringType(), False),
        StructField("driver_id", StringType(), False),
        StructField("rider_id", StringType(), False),
        StructField("vehicle_type", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("pickup_lat", DoubleType(), True),
        StructField("pickup_lon", DoubleType(), True),
        StructField("dropoff_lat", DoubleType(), True),
        StructField("dropoff_lon", DoubleType(), True),
        StructField("trip_duration_sec", IntegerType(), True),
        StructField("base_fare_usd", DoubleType(), True),
        StructField("request_timestamp", TimestampType(), True),
        StructField("status", StringType(), True),
        StructField("fraud_label", StringType(), True),
    ]
)


def ingest_to_bronze(
    spark: SparkSession, input_path: str = "data/raw/trips.csv", output_path: str = "data/bronze/trips"
) -> None:
    """
    Reads RAW CSV → writes Bronze Delta table.
    Bronze = raw data, no transformation, only schema enforcement.
    """
    print(f"Reading raw CSV from {input_path}...")

    df = (
        spark.read.option("header", "true")
        .option("enforceSchema", "false")
        .option("mode", "FAILFAST")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .schema(SCHEMA)
        .csv(input_path)
    )

    print(f"Row count: {df.count():,}")
    print("Schema:")
    df.printSchema()

    print(f"Writing Bronze Delta table -> {output_path}")
    (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(output_path))

    print("Bronze layer complete.")


if __name__ == "__main__":
    from src.spark_session import get_spark_session

    spark = get_spark_session("Ingestion-Bronze")
    try:
        ingest_to_bronze(spark)
    finally:
        spark.stop()
