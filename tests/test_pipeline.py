import pandas as pd
import pytest

from src.data_generator import generate
from src.ingestion import ingest_to_bronze
from src.pipeline import run_pipeline
from tests.test_transformations import trip


def test_generator_reproducible(tmp_path):
    first, second = tmp_path / "a.csv", tmp_path / "b.csv"
    generate(100, str(first), seed=7)
    generate(100, str(second), seed=7)
    assert first.read_bytes() == second.read_bytes()
    df = pd.read_csv(first)
    assert len(df) == df.trip_id.nunique() == 100
    assert (df.fraud_label != "none").sum() == 3
    with pytest.raises(ValueError):
        generate(0, str(first))


def test_full_pipeline_and_repeat_run(spark, tmp_path):
    raw = tmp_path / "raw"
    raw.mkdir()
    rows = [
        trip(),
        trip(),
        trip(trip_id="ghost", dropoff_lon=0.0, fraud_label="ghost_trip"),
        trip(trip_id="cancelled", status="cancelled"),
    ]
    pd.DataFrame(rows).to_csv(raw / "trips.csv", index=False)
    expected = {"bronze": 4, "silver": 2, "gold": 1}
    assert run_pipeline(spark, str(tmp_path)) == expected
    assert run_pipeline(spark, str(tmp_path)) == expected
    gold = spark.read.format("delta").load(str(tmp_path / "gold/trip_intensity")).first()
    assert gold.trip_count == 1
    evaluation = spark.read.format("delta").load(str(tmp_path / "gold/fraud_evaluation"))
    assert sum(row["count"] for row in evaluation.collect()) == 2


def test_wrong_csv_header_fails(spark, tmp_path):
    raw = tmp_path / "wrong.csv"
    pd.DataFrame([trip()]).rename(columns={"trip_id": "wrong_id"}).to_csv(raw, index=False)
    with pytest.raises(Exception, match="CSV header does not conform"):
        ingest_to_bronze(spark, str(raw), str(tmp_path / "bronze"))
