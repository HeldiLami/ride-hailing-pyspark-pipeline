from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "RideHailingPipeline") -> SparkSession:
    """
    Spark session initialize with Delta Lake support.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "8")  # i ulët për lokal
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


if __name__ == "__main__":
    spark = get_spark_session()
    print(f"Spark version: {spark.version}")
    spark.stop()