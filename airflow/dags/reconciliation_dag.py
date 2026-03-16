from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import pandas as pd
import psycopg2

from airflow import DAG
from airflow.operators.python import PythonOperator


# These will work if Airflow runs in docker-compose on same network as Postgres service "postgres"
PG_HOST = os.environ.get("PG_HOST", "postgres")
PG_PORT = int(os.environ.get("PG_PORT", "5432"))
PG_DB = os.environ.get("PG_DB", "fintech_db")
PG_USER = os.environ.get("PG_USER", "fintech")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "fintech")

# IMPORTANT: In docker-compose, mount ./reports to /opt/airflow/reports
REPORTS_DIR = os.environ.get("REPORTS_DIR", "/opt/airflow/reports")


def _get_conn():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )


def generate_reports(**context):
    """
    Generates 2 CSV reports for the last 6 hours:
    1) reconciliation report: ingress vs validated vs fraud sums
    2) fraud attempts by merchant category
    """
    # Airflow provides logical_date; we compute [end-6h, end)
    logical_date: datetime = context["logical_date"]
    end_ts = logical_date.astimezone(timezone.utc)
    start_ts = end_ts - timedelta(hours=6)

    os.makedirs(REPORTS_DIR, exist_ok=True)

    with _get_conn() as conn:
        # 1) Reconciliation numbers
        recon_sql = """
        SELECT
          %s::timestamptz AS window_start,
          %s::timestamptz AS window_end,
          COALESCE((SELECT SUM(amount) FROM raw_transactions
                    WHERE event_time >= %s AND event_time < %s), 0) AS total_ingress_amount,
          COALESCE((SELECT SUM(amount) FROM validated_transactions
                    WHERE event_time >= %s AND event_time < %s), 0) AS validated_amount,
          COALESCE((SELECT SUM(amount) FROM fraud_alerts
                    WHERE event_time >= %s AND event_time < %s), 0) AS fraud_amount
        ;
        """
        recon_df = pd.read_sql_query(
            recon_sql,
            conn,
            params=[start_ts, end_ts, start_ts, end_ts, start_ts, end_ts, start_ts, end_ts],
        )
        recon_df["difference_ingress_minus_validated"] = (
            recon_df["total_ingress_amount"] - recon_df["validated_amount"]
        )

        # 2) Fraud by merchant category
        fraud_cat_sql = """
        SELECT
          merchant_category,
          COUNT(*) AS fraud_attempts,
          COALESCE(SUM(amount), 0) AS fraud_amount
        FROM fraud_alerts
        WHERE event_time >= %s AND event_time < %s
        GROUP BY merchant_category
        ORDER BY fraud_attempts DESC, fraud_amount DESC;
        """
        fraud_cat_df = pd.read_sql_query(
            fraud_cat_sql,
            conn,
            params=[start_ts, end_ts],
        )

    # Filenames include window end time to avoid overwrite
    stamp = end_ts.strftime("%Y%m%dT%H%M%SZ")
    recon_path = os.path.join(REPORTS_DIR, f"reconciliation_{stamp}.csv")
    fraud_cat_path = os.path.join(REPORTS_DIR, f"fraud_by_category_{stamp}.csv")

    recon_df.to_csv(recon_path, index=False)
    fraud_cat_df.to_csv(fraud_cat_path, index=False)

    print(f"Wrote: {recon_path}")
    print(f"Wrote: {fraud_cat_path}")


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="fintech_reconciliation_reports",
    description="Every 6 hours generate reconciliation and fraud category reports from Postgres",
    default_args=default_args,
    start_date=datetime(2026, 3, 15),  # set to today to avoid confusion
    schedule="0 */6 * * *",  # every 6 hours
    catchup=False,
    tags=["fintech", "reports"],
) as dag:

    generate = PythonOperator(
        task_id="generate_reports",
        python_callable=generate_reports,
        provide_context=True,
    )

    generate