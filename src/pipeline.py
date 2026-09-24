"""Run the batch pipeline with python -m src.pipeline."""

import argparse
import logging
from pathlib import Path
from time import perf_counter

from src.data_generator import generate
from src.ingestion import ingest_to_bronze
from src.spark_session import get_spark_session
from src.transformations import bronze_to_silver, evaluate_fraud, silver_to_gold

logger = logging.getLogger(__name__)


def run_pipeline(spark, data_dir: str = "data") -> dict:
    root = Path(data_dir)
    started = perf_counter()
    bronze = str(root / "bronze/trips")
    silver_path = str(root / "silver/trips")
    ingest_to_bronze(spark, str(root / "raw/trips.csv"), bronze)
    silver = bronze_to_silver(spark, bronze, silver_path)
    gold = silver_to_gold(spark, silver_path, str(root / "gold/trip_intensity"))
    evaluation = evaluate_fraud(silver)
    evaluation.write.format("delta").mode("overwrite").save(str(root / "gold/fraud_evaluation"))
    counts = {
        "bronze": spark.read.format("delta").load(bronze).count(),
        "silver": silver.count(),
        "gold": gold.count(),
    }
    logger.info("Rows: %s; elapsed: %.2fs", counts, perf_counter() - started)
    gold.orderBy("request_15min", "intensity_rank", "geo_grid_key").show(10, truncate=False)
    evaluation.orderBy("fraud_label", "is_fraud").show(truncate=False)
    return counts


def main():
    parser = argparse.ArgumentParser(description="Batch ride-hailing analytics pipeline")
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--generate", action="store_true", help="Generate/replace raw synthetic CSV")
    parser.add_argument("--trips", type=int, default=10000)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    if args.trips <= 0:
        parser.error("--trips must be positive")
    raw = Path(args.data_dir) / "raw/trips.csv"
    if args.generate:
        generate(args.trips, str(raw), seed=args.seed)
    if not raw.is_file():
        parser.error(f"Missing {raw}. Use --generate or supply a CSV with the documented schema.")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    spark = get_spark_session()
    try:
        run_pipeline(spark, args.data_dir)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
