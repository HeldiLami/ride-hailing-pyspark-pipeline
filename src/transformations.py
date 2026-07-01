"""
Transformations: Bronze → Silver → Gold
- Silver: cleaning, fraud detection with lag/lead, geo grid
- Gold: surge pricing metrics, aggregations finale
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ── Constants ──────────────────────────────────────────────────────────────────
EARTH_RADIUS_KM = 6371.0
MAX_SPEED_KMH   = 150.0  
GEO_PRECISION   = 2      
SURGE_WINDOW_MIN = 15


# ── Silver Layer ───────────────────────────────────────────────────────────────
def bronze_to_silver(spark: SparkSession,
                     input_path:  str = "data/bronze/trips",
                     output_path: str = "data/silver/trips") -> DataFrame:
    """
    Bronze → Silver:
    1. Cleaning (null check, status filter)
    2. Haversine distance (km)
    3. Fraud detection with lag/lead (speed anomaly detection)
    4. Geo grid key for surge pricing
    """
    print("Reading Bronze...")
    df = spark.read.format("delta").load(input_path)

    # 1. Filter cancelled/no_show and null coordinates
    df = df.filter(
        (F.col("status") == "completed") &
        F.col("pickup_lat").isNotNull() &
        F.col("dropoff_lat").isNotNull()
    )

    # 2. Haversine distance (pickup → dropoff)
    df = _add_haversine_distance(df)

    # 3. Speed km/h
    df = df.withColumn(
        "speed_kmh",
        F.round(
            F.col("distance_km") / (F.col("trip_duration_sec") / 3600),
            2
        )
    )

    # 4. Fraud detection — speed anomaly + ghost trip
    df = df.withColumn(
        "is_fraud",
        F.when(F.col("speed_kmh") > MAX_SPEED_KMH, F.lit("speed_anomaly"))
         .when(F.col("distance_km") < 0.01,         F.lit("ghost_trip"))
         .otherwise(F.lit("none"))
    )

    # 5. Geo grid key
    df = df.withColumn(
        "geo_grid_key",
        F.concat(
            F.round(F.col("pickup_lat"), GEO_PRECISION).cast("string"),
            F.lit("_"),
            F.round(F.col("pickup_lon"), GEO_PRECISION).cast("string")
        )
    )

    # 6. Request time as timestamp
    df = df.withColumn(
        "request_hour",
        F.date_trunc("hour", F.col("request_timestamp"))
    ).withColumn(
        "request_15min",
        (F.unix_timestamp("request_timestamp") - 
         F.unix_timestamp("request_timestamp") % (SURGE_WINDOW_MIN * 60)
        ).cast("timestamp")
    )

    print(f"[silver] Writing Silver Delta → {output_path}")
    df.write.format("delta").mode("overwrite").save(output_path)
    print("[silver] Silver layer complete.")
    return df


def _add_haversine_distance(df: DataFrame) -> DataFrame:
    """
    Calculates Haversine distance (km) between pickup dhe dropoff.
    Standard geographic formula for the distance between two coordinates.
    """
    lat1 = F.radians(F.col("pickup_lat"))
    lat2 = F.radians(F.col("dropoff_lat"))
    lon1 = F.radians(F.col("pickup_lon"))
    lon2 = F.radians(F.col("dropoff_lon"))

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = (F.sin(dlat / 2) ** 2 +
         F.cos(lat1) * F.cos(lat2) * F.sin(dlon / 2) ** 2)

    return df.withColumn(
        "distance_km",
        F.round(2 * EARTH_RADIUS_KM * F.asin(F.sqrt(a)), 3)
    )


# ── Gold Layer ─────────────────────────────────────────────────────────────────
def silver_to_gold(spark: SparkSession,
                   input_path:  str = "data/silver/trips",
                   output_path: str = "data/gold/surge_metrics") -> DataFrame:
    """
    Silver → Gold:
    1. Surge pricing: demand vs drivers in each zone + 15 min window
    2. Window functions for ranking and metrics
    """
    print("[gold] Reading Silver...")
    df = spark.read.format("delta").load(input_path)

    # Filter fraud from gold layer
    df = df.filter(F.col("is_fraud") == "none")

    # 1. Unique trips and drivers for each zone + 15 min window
    zone_window = ["geo_grid_key", "request_15min"]

    df_agg = df.groupBy(*zone_window).agg(
        F.count("trip_id").alias("trip_count"),
        F.countDistinct("driver_id").alias("active_drivers"),
        F.avg("base_fare_usd").alias("avg_fare"),
        F.avg("distance_km").alias("avg_distance_km"),
        F.avg("speed_kmh").alias("avg_speed_kmh")
    )

    # 2. Surge multiplier: demand/supply ratio
    df_agg = df_agg.withColumn(
        "demand_supply_ratio",
        F.round(F.col("trip_count") / F.col("active_drivers"), 2)
    ).withColumn(
        "surge_multiplier",
        F.when(F.col("demand_supply_ratio") > 3.0, F.lit(2.5))
         .when(F.col("demand_supply_ratio") > 2.0, F.lit(1.8))
         .when(F.col("demand_supply_ratio") > 1.5, F.lit(1.4))
         .when(F.col("demand_supply_ratio") > 1.0, F.lit(1.2))
         .otherwise(F.lit(1.0))
    )

    # 3. Window function — zone ranking with the most surge
    w = Window.partitionBy("request_15min").orderBy(F.col("surge_multiplier").desc())
    df_agg = df_agg.withColumn("surge_rank", F.rank().over(w))

    print(f"Writing Gold Delta → {output_path}")
    df_agg.write.format("delta").mode("overwrite").save(output_path)
    print("Gold layer complete.")

    print("\nTop 10 surge zones:")
    df_agg.filter(F.col("surge_rank") == 1).show(10, truncate=False)

    return df_agg


# ── Pipeline Runner ────────────────────────────────────────────────────────────
if __name__ == "__main__":
    from spark_session import get_spark_session
    spark = get_spark_session("Transformations")

    silver_df = bronze_to_silver(spark)
    gold_df   = silver_to_gold(spark)

    spark.stop()