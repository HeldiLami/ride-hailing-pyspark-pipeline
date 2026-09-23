"""
Mock data generator for Ride-Hailing Fraud & Trip Intensity Analytics.
Generates ~2 million GPS trip records simulating Uber/Bolt data.
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta

SEED = 42
NUM_TRIPS = 2_000_000
OUTPUT_PATH = "data/raw/trips.csv"

LAT_MIN, LAT_MAX = 40.4774, 40.9176
LON_MIN, LON_MAX = -74.2591, -73.7004
FRAUD_RATE = 0.03


def random_timestamps(n, rng, start="2024-01-01", days=90):
    base = datetime.strptime(start, "%Y-%m-%d")
    offsets = rng.integers(0, days * 24 * 3600, size=n)
    return [base + timedelta(seconds=int(o)) for o in offsets]


def inject_fraud(df, rng):
    fraud_idx = rng.choice(df.index, size=round(len(df) * FRAUD_RATE), replace=False)
    half = len(fraud_idx) // 2

    ghost_idx = fraud_idx[:half]
    df.loc[ghost_idx, "dropoff_lat"] = df.loc[ghost_idx, "pickup_lat"]
    df.loc[ghost_idx, "dropoff_lon"] = df.loc[ghost_idx, "pickup_lon"]
    df.loc[ghost_idx, "trip_duration_sec"] = rng.integers(30, 120, size=len(ghost_idx))
    df.loc[ghost_idx, "fraud_label"] = "ghost_trip"

    speed_idx = fraud_idx[half:]
    df.loc[speed_idx, "dropoff_lat"] = df.loc[speed_idx, "pickup_lat"] + rng.uniform(
        0.5, 1.2, size=len(speed_idx)
    )
    df.loc[speed_idx, "dropoff_lon"] = df.loc[speed_idx, "pickup_lon"] + rng.uniform(
        0.5, 1.2, size=len(speed_idx)
    )
    df.loc[speed_idx, "trip_duration_sec"] = rng.integers(5, 30, size=len(speed_idx))
    df.loc[speed_idx, "fraud_label"] = "speed_anomaly"

    return df


def generate(n=NUM_TRIPS, output=OUTPUT_PATH, seed=SEED):
    if n <= 0:
        raise ValueError("n must be positive")
    rng = np.random.default_rng(seed)
    print(f"[generator] Generating {n:,} trip records...")

    driver_ids = [f"DRV-{i:05d}" for i in range(1, 5001)]
    rider_ids = [f"RDR-{i:06d}" for i in range(1, 50001)]
    vehicle_types = ["UberX", "UberXL", "Bolt", "BoltXL", "Economy", "Premium"]
    payment_methods = ["card", "cash", "wallet"]

    pickup_lat = rng.uniform(LAT_MIN, LAT_MAX, n)
    pickup_lon = rng.uniform(LON_MIN, LON_MAX, n)
    dropoff_lat = pickup_lat + rng.uniform(-0.05, 0.05, n)
    dropoff_lon = pickup_lon + rng.uniform(-0.05, 0.05, n)

    trip_duration_sec = rng.integers(120, 3600, n)
    base_fare = np.round(rng.uniform(3.0, 45.0, n), 2)

    timestamps = random_timestamps(n, rng)

    df = pd.DataFrame(
        {
            "trip_id": [f"TRIP-{i:07d}" for i in range(n)],
            "driver_id": rng.choice(driver_ids, n),
            "rider_id": rng.choice(rider_ids, n),
            "vehicle_type": rng.choice(vehicle_types, n),
            "payment_method": rng.choice(payment_methods, n),
            "pickup_lat": np.round(pickup_lat, 6),
            "pickup_lon": np.round(pickup_lon, 6),
            "dropoff_lat": np.round(dropoff_lat, 6),
            "dropoff_lon": np.round(dropoff_lon, 6),
            "trip_duration_sec": trip_duration_sec,
            "base_fare_usd": base_fare,
            "request_timestamp": timestamps,
            "status": rng.choice(["completed", "cancelled", "no_show"], n, p=[0.85, 0.10, 0.05]),
            "fraud_label": "none",
        }
    )

    df = inject_fraud(df, rng)
    Path(output).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output, index=False)
    print(f"Saved {len(df):,} records -> {output}")
    print(f"Fraud breakdown:\n{df['fraud_label'].value_counts()}")


if __name__ == "__main__":
    generate()
