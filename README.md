# Near-Real-Time Rides Analytics Pipeline

## Overview
This project implements a **near-real-time analytics pipeline** for ride-hailing–style event data using **Kafka** and **Spark Structured Streaming**, following a **Bronze → Silver → Gold** lakehouse architecture.

The pipeline ingests streaming ride events, handles **late-arriving and duplicate data**, enforces **event-time semantics**, and produces **windowed analytical aggregates** suitable for BI and ad-hoc SQL analysis.

The project is designed to demonstrate a clear understanding of the differences between **batch and streaming systems**, operational failure modes, and how real-world data pipelines are built and maintained.

---

## Architecture

```
Event Generator (Python)
        ↓
Kafka (Docker)
        ↓
Spark Structured Streaming
   ├── Bronze: raw immutable events
   ├── Silver: cleaned & deduplicated events
   └── Gold: windowed analytics
        ↓
Parquet (S3-compatible layout / local FS)
```

---

## Technology Stack

- **Streaming platform:** Apache Kafka (Docker)
- **Stream processing:** Spark Structured Streaming
- **Storage format:** Parquet (designed to be upgraded to Iceberg)
- **Languages:** Python (PySpark)
- **Infrastructure:** Docker Compose
- **Environment management:** Java 11 + Python 3.9

---

## Data Model

### Event Types
Each ride produces multiple events over its lifecycle:

- `ride_requested`
- `ride_accepted`
- `ride_completed`
- `fare_updated` (may arrive late)

### Event Schema (simplified)
```json
{
  "event_id": "uuid",
  "event_type": "ride_completed",
  "event_time": "2026-01-22T10:05:23Z",
  "ride_id": "uuid",
  "user_id": "uuid",
  "driver_id": "uuid",
  "city": "Auckland",
  "payload": {
    "fare": 23.40,
    "duration_seconds": 780,
    "distance_km": 6.2
  }
}
```

---

## Bronze Layer — Raw Streaming Ingestion

**Purpose:** Preserve an immutable, replayable record of all incoming events.

### Characteristics
- Ingests directly from Kafka
- Append-only
- No deduplication or validation
- Includes Kafka metadata (partition, offset)
- Accepts all late events

### Why this matters
The Bronze layer acts as the **source of truth**. If downstream logic changes or errors occur, the pipeline can be **replayed safely** from Bronze.

---

## Silver Layer — Clean & Reliable Events

**Purpose:** Produce trustworthy, schema-consistent event data.

### Processing logic
- Parses `event_time` into proper timestamps
- Applies **event-time watermarks** (2 hours)
- Deduplicates events using `event_id`
- Explicitly casts all numeric fields
- Drops malformed records

### Why explicit casting matters
During development, schema drift occurred when numeric fields were implicitly treated as strings. The pipeline was updated to **explicitly cast all fields**, preventing downstream failures — a common real-world streaming issue.

---

## Gold Layer — Analytics & Aggregations

**Purpose:** Provide business-ready, queryable metrics.

### Metrics produced
Per **city**, per **5-minute tumbling window**:
- Number of completed rides
- Average fare
- 95th percentile ride duration

### Event-time handling
- Aggregations are based on `event_time`, not ingestion time
- Late events within the watermark update results
- Windows are emitted once closed, ensuring idempotent outputs

---

## Late-Arriving Data Strategy

- **Within watermark:** events update Silver and Gold results
- **Beyond watermark:** events remain in Bronze for audit and batch reconciliation

This mirrors how production streaming systems balance correctness and bounded state.

---

## Exactly-Once Semantics (Practical Approach)

- Kafka offsets are tracked via Spark checkpoints
- Writes are append-only and partitioned
- Outputs are designed to be idempotent

If Kafka data is reset in local development, checkpoints are intentionally cleared to maintain correctness — reflecting real operational workflows.

---

## Local Development Workflow

### One-time setup
```bash
brew install openjdk@11 python@3.9
pip install pyspark==3.4.1 confluent-kafka
```

### Start environment
```bash
source scripts/env.sh
./scripts/start_infra.sh
./scripts/start_pipeline.sh
```

### Restart rules (important)
- Kafka reset → reset **Bronze** checkpoints
- Bronze reset → reset **Silver** checkpoints
- Silver reset → reset **Gold** checkpoints

This prevents offset and schema inconsistencies.

---

## Repository Structure

```
streaming-pipeline/
├── docker/
│   └── docker-compose.yml
├── producer/
│   └── ride_event_generator.py
├── spark/
│   ├── bronze_stream.py
│   ├── silver_stream.py
│   └── gold_aggregations.py
├── scripts/
│   ├── env.sh
│   ├── start_infra.sh
│   └── start_pipeline.sh
├── data/
├── checkpoints/
└── README.md
```

---

## Key Learnings

- Streaming pipelines fail *loudly* by design — this prevents silent data loss
- Schema drift is one of the biggest risks in streaming systems
- Checkpoints are as important as data
- Event-time semantics are essential for correct analytics
- Local development requires explicit operational discipline

---

## Future Improvements

- Upgrade Parquet tables to **Apache Iceberg** (ACID, schema evolution)
- Add **Airflow batch reconciliation** jobs
- Persist Kafka data using Docker volumes
- Expose Gold tables via Athena / Trino
- Add data quality metrics and alerting
