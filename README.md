# Ride-Hailing Fraud and Surge Pricing Engine

A production-style Data Engineering pipeline simulating real-world Uber/Bolt data infrastructure built with PySpark, Delta Lake, and Docker.

---

## Architecture and Data Flow

```mermaid
graph TD
    Raw["Raw CSV (2M trips)"] --> Ingestion["Ingestion with Schema Enforcement (Delta Lake)"]

    Ingestion --> Bronze["[Bronze Layer]"]

    Bronze --> Transform1["Cleaning, Haversine Distance, Fraud Detection"]

    Transform1 --> Silver["[Silver Layer]"]

    Silver --> Transform2["Surge Pricing Metrics, Window Functions, Zone Ranking"]

    Transform2 --> Gold["[Gold Layer]"]

    style Raw fill:#1e293b,stroke:#475569,stroke-width:2px,color:#fff
    style Ingestion fill:#0f172a,stroke:#38bdf8,stroke-width:1px,color:#94a3b8
    style Bronze fill:#78350f,stroke:#b45309,stroke-width:2px,color:#fff
    style Transform1 fill:#0f172a,stroke:#38bdf8,stroke-width:1px,color:#94a3b8
    style Silver fill:#334155,stroke:#64748b,stroke-width:2px,color:#fff
    style Transform2 fill:#0f172a,stroke:#4ade80,stroke-width:1px,color:#94a3b8
    style Gold fill:#14532d,stroke:#22c55e,stroke-width:2px,color:#fff
```

---

## Tech Stack

| Tool               | Purpose                        |
| :----------------- | :----------------------------- |
| **PySpark 3.5**    | Distributed data processing    |
| **Delta Lake 3.1** | ACID transactions, time travel |
| **Docker**         | Reproducible environment       |
| **Python 3.11**    | Pipeline orchestration         |

---

## Key Features

### Fraud Detection

- **ghost_trip**: driver coordinates unchanged after trip completion.
- **speed_anomaly**: computed via Haversine formula, flags trips exceeding 150 km/h.

### Surge Pricing Engine

- Demand/supply ratio per geographic grid zone (2 decimal lat/lon precision).
- 15-minute rolling time windows.
- Dynamic multiplier: 1.0x to 2.5x based on zone saturation.

### Medallion Architecture

- **Bronze**: raw data preserved as-is.
- **Silver**: cleaned, enriched, fraud-labeled.
- **Gold**: business-ready aggregations.

---

## Quick Start

Prerequisites: Docker Desktop

```bash
git clone https://github.com/YOUR_USERNAME/ride-hailing-pipeline.git
cd ride-hailing-pipeline

# Generate 2M mock trip records
docker build -t ride-hailing-pipeline .
docker run --rm -v ${PWD}/data:/app/data ride-hailing-pipeline python src/data_generator.py

# Run full pipeline: Bronze -> Silver -> Gold
docker run --rm -v ${PWD}/data:/app/data ride-hailing-pipeline python src/transformations.py
```

---

## Project Structure

```text
ride-hailing-pipeline/
├── data/
│   ├── raw/                 # Generated CSV (gitignored)
│   ├── bronze/              # Delta Lake - raw ingested
│   ├── silver/              # Delta Lake - cleaned + fraud labeled
│   └── gold/                # Delta Lake - surge metrics
├── src/
│   ├── spark_session.py     # SparkSession + Delta config
│   ├── data_generator.py    # Mock data generation (2M records)
│   ├── ingestion.py         # CSV -> Bronze
│   └── transformations.py   # Bronze -> Silver -> Gold
├── tests/
│   └── test_transformations.py
├── notebooks/
│   └── eda.ipynb
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── .gitignore
```

---

## Dataset

2,000,000 synthetic trip records with:

- GPS coordinates within NYC bounding box.
- 5,000 unique drivers / 50,000 unique riders.
- ~3% injected fraud patterns (ghost trips + speed anomalies).
- 90-day time range.
