"""Pure DataFrame transformations and Delta Lake layer writers."""

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

EARTH_RADIUS_KM = 6371.0
MAX_SPEED_KMH = 150.0
GEO_PRECISION = 2
WINDOW_MINUTES = 15


def clean_trips(df: DataFrame) -> DataFrame:
    """Keep valid completed trips; exclude conflicting versions of a trip ID."""
    valid = (F.col("status") == "completed") & F.col("request_timestamp").isNotNull()
    for name in ("trip_id", "driver_id", "rider_id"):
        valid = valid & (F.length(F.trim(F.col(name))) > 0)
    for name, bound in (("pickup_lat", 90), ("dropoff_lat", 90), ("pickup_lon", 180), ("dropoff_lon", 180)):
        valid = valid & F.col(name).between(-bound, bound)
    valid = valid & (F.col("trip_duration_sec") > 0)
    valid = valid & (F.col("base_fare_usd") >= 0) & (F.col("base_fare_usd") < float("inf"))
    distinct = df.dropDuplicates()
    counts = distinct.groupBy("trip_id").count().filter(F.col("count") == 1).select("trip_id")
    return distinct.join(counts, "trip_id", "inner").filter(valid)


def add_trip_features(df: DataFrame) -> DataFrame:
    """Add straight-line distance, speed, grid and fixed UTC time buckets."""
    lat1, lat2 = F.radians("pickup_lat"), F.radians("dropoff_lat")
    lon1, lon2 = F.radians("pickup_lon"), F.radians("dropoff_lon")
    a = F.sin((lat2 - lat1) / 2) ** 2 + F.cos(lat1) * F.cos(lat2) * F.sin((lon2 - lon1) / 2) ** 2
    distance = 2 * EARTH_RADIUS_KM * F.asin(F.sqrt(F.least(F.lit(1.0), F.greatest(F.lit(0.0), a))))
    return (
        df.withColumn("distance_km", distance)
        .withColumn("speed_kmh", F.col("distance_km") * 3600 / F.col("trip_duration_sec"))
        .withColumn(
            "geo_grid_key",
            F.concat_ws(
                "_",
                F.round("pickup_lat", GEO_PRECISION).cast("string"),
                F.round("pickup_lon", GEO_PRECISION).cast("string"),
            ),
        )
        .withColumn(
            "request_15min", F.window("request_timestamp", f"{WINDOW_MINUTES} minutes").getField("start")
        )
    )


def detect_fraud(df: DataFrame) -> DataFrame:
    """Flag suspicious trips using features, never the synthetic ground truth."""
    return df.withColumn(
        "is_fraud",
        F.when(F.col("speed_kmh") > MAX_SPEED_KMH, "speed_anomaly")
        .when(F.col("distance_km") < 0.01, "ghost_trip")
        .otherwise("none"),
    )


def build_intensity_metrics(df: DataFrame) -> DataFrame:
    """Count unflagged completed trips per zone and quarter-hour; rank ties equally."""
    metrics = (
        df.filter(F.col("is_fraud") == "none")
        .groupBy("geo_grid_key", "request_15min")
        .agg(
            F.count("trip_id").alias("trip_count"),
            F.countDistinct("driver_id").alias("unique_drivers"),
            F.avg("base_fare_usd").alias("avg_fare_usd"),
            F.avg("distance_km").alias("avg_distance_km"),
        )
    )
    rank_window = Window.partitionBy("request_15min").orderBy(F.desc("trip_count"))
    return metrics.withColumn("intensity_rank", F.dense_rank().over(rank_window))


def evaluate_fraud(df: DataFrame) -> DataFrame:
    """Confusion counts for retained Silver trips (synthetic labels only)."""
    return df.groupBy("fraud_label", "is_fraud").count()


def bronze_to_silver(
    spark: SparkSession, input_path: str = "data/bronze/trips", output_path: str = "data/silver/trips"
) -> DataFrame:
    df = detect_fraud(add_trip_features(clean_trips(spark.read.format("delta").load(input_path))))
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(output_path)
    return spark.read.format("delta").load(output_path)


def silver_to_gold(
    spark: SparkSession, input_path: str = "data/silver/trips", output_path: str = "data/gold/trip_intensity"
) -> DataFrame:
    df = build_intensity_metrics(spark.read.format("delta").load(input_path))
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(output_path)
    return spark.read.format("delta").load(output_path)
