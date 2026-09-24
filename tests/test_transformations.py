from datetime import datetime

import pytest

from src.ingestion import SCHEMA
from src.transformations import (
    add_trip_features,
    build_intensity_metrics,
    clean_trips,
    detect_fraud,
)


def trip(**updates):
    row = dict(
        trip_id="t1",
        driver_id="d1",
        rider_id="r1",
        vehicle_type="Economy",
        payment_method="card",
        pickup_lat=0.0,
        pickup_lon=0.0,
        dropoff_lat=0.0,
        dropoff_lon=1.0,
        trip_duration_sec=3600,
        base_fare_usd=10.0,
        request_timestamp=datetime(2024, 1, 1, 0, 14, 59),
        status="completed",
        fraud_label="none",
    )
    row.update(updates)
    return row


def test_distance_speed_and_fixed_windows(spark):
    df = spark.createDataFrame(
        [trip(), trip(trip_id="t2", request_timestamp=datetime(2024, 1, 1, 0, 15))], SCHEMA
    )
    rows = add_trip_features(df).orderBy("trip_id").collect()
    assert rows[0].distance_km == pytest.approx(111.195, abs=0.001)
    assert rows[0].speed_kmh == pytest.approx(111.195, abs=0.001)
    assert rows[0].request_15min == datetime(2024, 1, 1, 0, 0)
    assert rows[1].request_15min == datetime(2024, 1, 1, 0, 15)


def test_validation_and_duplicate_policy(spark):
    invalid = [
        {"pickup_lon": None},
        {"dropoff_lon": 181.0},
        {"pickup_lat": float("nan")},
        {"trip_duration_sec": 0},
        {"trip_duration_sec": -1},
        {"base_fare_usd": -1.0},
        {"base_fare_usd": float("nan")},
        {"base_fare_usd": float("inf")},
        {"request_timestamp": None},
        {"driver_id": ""},
        {"status": "cancelled"},
        {"rider_id": "  "},
    ]
    rows = [trip(), trip()]
    rows += [trip(trip_id=f"bad{i}", **fields) for i, fields in enumerate(invalid)]
    rows += [trip(trip_id="conflict"), trip(trip_id="conflict", base_fare_usd=20.0)]
    result = clean_trips(spark.createDataFrame(rows, SCHEMA)).collect()
    assert [r.trip_id for r in result] == ["t1"]


def test_fraud_rules_ignore_ground_truth(spark):
    rows = [
        trip(fraud_label="ghost_trip"),
        trip(trip_id="ghost", dropoff_lon=0.0),
        trip(trip_id="fast", trip_duration_sec=60),
    ]
    result = detect_fraud(add_trip_features(spark.createDataFrame(rows, SCHEMA)))
    assert {r.trip_id: r.is_fraud for r in result.collect()} == {
        "t1": "none",
        "ghost": "ghost_trip",
        "fast": "speed_anomaly",
    }


def test_fraud_thresholds(spark):
    df = spark.createDataFrame(
        [(150.0, 1.0), (150.01, 1.0), (0.0, 0.009), (1.0, 0.01)], ["speed_kmh", "distance_km"]
    )
    assert [r.is_fraud for r in detect_fraud(df).collect()] == ["none", "speed_anomaly", "ghost_trip", "none"]


def test_intensity_counts_excludes_fraud_and_ranks_ties(spark):
    timestamp = datetime(2024, 1, 1)
    rows = [
        ("a", timestamp, "t1", "d1", 10.0, 1.0, "none"),
        ("a", timestamp, "t2", "d1", 20.0, 3.0, "none"),
        ("b", timestamp, "t3", "d2", 10.0, 1.0, "none"),
        ("c", timestamp, "t4", "d3", 10.0, 1.0, "none"),
        ("b", timestamp, "t5", "d4", 10.0, 1.0, "ghost_trip"),
    ]
    df = spark.createDataFrame(
        rows,
        ["geo_grid_key", "request_15min", "trip_id", "driver_id", "base_fare_usd", "distance_km", "is_fraud"],
    )
    result = {r.geo_grid_key: r for r in build_intensity_metrics(df).collect()}
    assert result["a"].trip_count == 2
    assert result["a"].unique_drivers == 1
    assert result["a"].avg_fare_usd == 15.0
    assert result["a"].avg_distance_km == 2.0
    assert result["a"].intensity_rank == 1
    assert result["b"].trip_count == 1
    assert result["b"].intensity_rank == result["c"].intensity_rank == 2
