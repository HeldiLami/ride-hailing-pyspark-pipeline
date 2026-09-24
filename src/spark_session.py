"""Local Spark configuration for reproducible batch processing."""

import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "RideHailingPipeline") -> SparkSession:
    builder = (
        SparkSession.builder.master(os.environ.get("SPARK_MASTER", "local[2]"))
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
