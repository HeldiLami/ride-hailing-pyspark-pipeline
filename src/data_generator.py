"""
Mock data generator for Ride-Hailing Fraud & Surge Pricing Engine.
Generates ~2 million GPS trip records simulating Uber/Bolt data.
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
import random

SEED = 42
NUM_TRIPS = 2_000_000
OUTPUT_PATH = "data/raw/trips.csv"

LAT_MIN, LAT_MAX = 40.4774, 40.9176
LON_MIN, LON_MAX = -74.2591, -73.7004
FRAUD_RATE = 0.03

np.random.seed(SEED)
random.seed(SEED)

def random_timestamps(n, start="2024-01-01", days=90):
    base = datetime.strptime(start, "%Y-%m-%d")
    offsets = np.random.randint(0, days * 24 * 3600, size=n)
    return [base + timedelta(seconds=int(o)) for o in offsets]

def inject_fraud(df):
    fraud_idx = df.sample(frac=FRAUD_RATE, random_state=SEED).index
    half = len(fraud_idx) // 2

    ghost_idx = fraud_idx[:half]
    df.loc[ghost_idx, "dropoff_lat"] = df.loc[ghost_idx, "pickup_lat"]
    df.loc[ghost_idx, "dropoff_lon"] = df.loc[ghost_idx, "pickup_lon"]
    df.loc[ghost_idx, "trip_duration_sec"] = np.random.randint(30, 120, size=len(ghost_idx))
    df.loc[ghost_idx, "fraud_label"] = "ghost_trip"

    speed_idx = fraud_idx[half:]
    df.loc[speed_idx, "dropoff_lat"] = df.loc[speed_idx, "pickup_lat"] + np.random.uniform(0.5, 1.2, size=len(speed_idx))
    df.loc[speed_idx, "dropoff_lon"] = df.loc[speed_idx, "pickup_lon"] + np.random.uniform(0.5, 1.2, size=len(speed_idx))
    df.loc[speed_idx, "trip_duration_sec"] = np.random.randint(5, 30, size=len(speed_idx))
    df.loc[speed_idx, "fraud_label"] = "speed_anomaly"

    return df

def generate(n=NUM_TRIPS, output=OUTPUT_PATH):
    print(f"[generator] Generating {n:,} trip records...")

    driver_ids = [f"DRV-{i:05d}" for i in range(1, 5001)]
    rider_ids = [f"RDR-{i:06d}" for i in range(1, 50001)]
    vehicle_types = ["UberX", "UberXL", "Bolt", "BoltXL", "Economy", "Premium"]
    payment_methods = ["card", "cash", "wallet"]

    pickup_lat = np.random.uniform(LAT_MIN, LAT_MAX, n)
    pickup_lon = np.random.uniform(LON_MIN, LON_MAX, n)
    dropoff_lat = pickup_lat + np.random.uniform(-0.05, 0.05, n)
    dropoff_lon = pickup_lon + np.random.uniform(-0.05, 0.05, n)

    trip_duration_sec = np.random.randint(120, 3600, n)
    base_fare = np.round(np.random.uniform(3.0, 45.0, n), 2)

    timestamps = random_timestamps(n)

    df = pd.DataFrame({
        "trip_id":           [f"TRIP-{i:07d}" for i in range(n)],
        "driver_id":         np.random.choice(driver_ids, n),
        "rider_id":          np.random.choice(rider_ids, n),
        "vehicle_type":      np.random.choice(vehicle_types, n),
        "payment_method":    np.random.choice(payment_methods, n),
        "pickup_lat":        np.round(pickup_lat, 6),
        "pickup_lon":        np.round(pickup_lon, 6),
        "dropoff_lat":       np.round(dropoff_lat, 6),
        "dropoff_lon":       np.round(dropoff_lon, 6),
        "trip_duration_sec": trip_duration_sec,
        "base_fare_usd":     base_fare,
        "surge_multiplier":  np.ones(n),
        "request_timestamp": timestamps,
        "status":            np.random.choice(["completed", "cancelled", "no_show"], n, p=[0.85, 0.10, 0.05]),
        "fraud_label":       "none",
    })

    df = inject_fraud(df)
    os.makedirs(os.path.dirname(output), exist_ok=True)
    df.to_csv(output, index=False)
    print(f"[generator] Saved {len(df):,} records → {output}")
    print(f"[generator] Fraud breakdown:\n{df['fraud_label'].value_counts()}")

if __name__ == "__main__":
    generate()