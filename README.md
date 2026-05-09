# Fintech Reports Pipeline

 Real-time fraud detection and batch reporting pipeline built with Kafka, Spark Structured Streaming, Postgres, and Airflow.

 ![Fintech workflow](https://github.com/user-attachments/assets/93f69c9c-a253-404b-9d26-e6d47b96a772)

 ## Workflow (high level)
 1. **Source**: A Python producer generates mock card transactions in JSON.
 2. **Kafka**: Transactions are published to a `transactions` topic.
 3. **Spark Streaming**: Spark parses JSON, applies event time + watermarking, and flags fraud:
    - `HIGH_AMOUNT` when amount exceeds the threshold.
    - `IMPOSSIBLE_TRAVEL` when the same user appears in two countries within 10 minutes.
 4. **Storage**: Raw and fraud data land in Postgres tables.
 5. **Airflow**: Every 6 hours, Airflow:
    - Builds a validated dataset (raw minus fraud) and stores it to Postgres + Parquet.
    - Generates CSV reports (reconciliation + fraud by category).
 6. **Reports**: CSV outputs are written to the `reports/` folder and Parquet outputs to `warehouse/`.

 ## Repository structure
 - `producers/` — Python Kafka producer for mock transactions.
 - `spark/` — Spark Structured Streaming fraud detection job.
 - `airflow/dags/` — Airflow DAGs for validation + reporting.
 - `sql/` — Postgres schema for raw, validated, and fraud tables.
 - `warehouse/` — Example Parquet outputs (validated datasets).
 - `docker-compose.core.yml` — Kafka + Zookeeper + Postgres.
 - `docker-compose.analytics.yml` — Airflow + Spark.

 ## Prerequisites
 - Docker + Docker Compose
 - Python 3.10+ (for running the producer locally)

 ## Quick start
 1. **Create the shared Docker network** (used by both compose files):
    ```bash
    docker network create fintech-net
    ```

 2. **Start core services (Kafka + Postgres)**:
    ```bash
    docker compose -f docker-compose.core.yml up -d
    ```

 3. **Start analytics services (Airflow + Spark)**:
    ```bash
    docker compose -f docker-compose.analytics.yml up -d
    ```

 4. **Create the Kafka topic**:
    ```bash
    docker exec -it fintech-kafka kafka-topics \
      --create --topic transactions --bootstrap-server localhost:9092 \
      --partitions 3 --replication-factor 1
    ```

 5. **Run the producer (host machine)**:
    ```bash
    pip install kafka-python
    python producers/transaction_producer.py
    ```

 6. **Run the Spark streaming job**:
    ```bash
    docker exec -it fintech-spark-master /opt/spark/bin/spark-submit \
      /opt/spark-apps/stream_fraud_detection.py
    ```

 7. **Trigger Airflow DAGs**:
    - Open Airflow UI: http://localhost:8080
    - Enable `fintech_etl_validate_and_reports_every_6_hours` (recommended)

 ## Outputs
 - **Postgres tables**:
   - `raw_transactions`
   - `fraud_alerts`
   - `validated_transactions`
 - **Reports**: `reports/reconciliation_*.csv`, `reports/fraud_by_category_*.csv`
 - **Warehouse**: `warehouse/validated/window_end=.../validated.parquet`

 ## Useful ports
 - Postgres: `5432`
 - Kafka: `9092` (host), `29092` (docker network)
 - Airflow UI: `8080`
 - Spark UI: `8081`
