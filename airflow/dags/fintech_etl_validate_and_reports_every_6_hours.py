from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import pandas as pd
import psycopg2

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


PG_HOST = os.environ.get("PG_HOST", "fintech-postgres")
PG_PORT = int(os.environ.get("PG_PORT", "5432"))
PG_DB = os.environ.get("PG_DB", "fintech_db")
PG_USER = os.environ.get("PG_USER", "fintech")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "fintech")

REPORTS_DIR = os.environ.get("REPORTS_DIR", "/opt/airflow/reports")
WAREHOUSE_DIR = os.environ.get("WAREHOUSE_DIR", "/opt/airflow/warehouse")


def _get_conn():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )


def build_validated_and_store(**context):
    """
    Every 6 hours:
      - validated = raw minus fraud (for that 6 hour window)
      - insert validated rows into validated_transactions
      - export validated rows to Parquet "warehouse"
    """
    logical_date: datetime = context["logical_date"]
    end_ts = logical_date.astimezone(timezone.utc)
    start_ts = end_ts - timedelta(hours=6)

    # validated = raw - fraud (window)
    sql = """
    WITH fraud_ids AS (
      SELECT DISTINCT transaction_id
      FROM fraud_alerts
      WHERE event_time >= %s AND event_time < %s
    )
    SELECT r.transaction_id, r.user_id, r.event_time, r.merchant_category, r.amount, r.country, r.city
    FROM raw_transactions r
    LEFT JOIN fraud_ids f ON r.transaction_id = f.transaction_id
    WHERE r.event_time >= %s AND r.event_time < %s
      AND f.transaction_id IS NULL
    ORDER BY r.event_time ASC;
    """

    with _get_conn() as conn:
        df = pd.read_sql_query(sql, conn, params=[start_ts, end_ts, start_ts, end_ts])

        insert_sql = """
        INSERT INTO validated_transactions (
          transaction_id, user_id, event_time, merchant_category, amount, country, city
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (transaction_id) DO NOTHING;
        """

        with conn.cursor() as cur:
            for row in df.itertuples(index=False):
                cur.execute(
                    insert_sql,
                    (
                        row.transaction_id,
                        row.user_id,
                        row.event_time,
                        row.merchant_category,
                        float(row.amount),
                        row.country,
                        row.city,
                    ),
                )
        conn.commit()

    # Export parquet
    os.makedirs(WAREHOUSE_DIR, exist_ok=True)
    stamp = end_ts.strftime("%Y%m%dT%H%M%SZ")
    out_dir = os.path.join(WAREHOUSE_DIR, "validated", f"window_end={stamp}")
    os.makedirs(out_dir, exist_ok=True)

    out_file = os.path.join(out_dir, "validated.parquet")
    df.to_parquet(out_file, index=False)

    print(f"[airflow] validated rows for window: {len(df)}")
    print(f"[airflow] wrote parquet: {out_file}")


def generate_reports(**context):
    """
    Generates:
      1) reconciliation report (ingress vs validated vs fraud)
      2) fraud attempts by merchant category
    """
    logical_date: datetime = context["logical_date"]
    end_ts = logical_date.astimezone(timezone.utc)
    start_ts = end_ts - timedelta(hours=6)

    os.makedirs(REPORTS_DIR, exist_ok=True)

    with _get_conn() as conn:
        recon_df = pd.read_sql_query(
            """
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
            """,
            conn,
            params=[start_ts, end_ts, start_ts, end_ts, start_ts, end_ts, start_ts, end_ts],
        )
        recon_df["difference_ingress_minus_validated"] = (
            recon_df["total_ingress_amount"] - recon_df["validated_amount"]
        )

        fraud_cat_df = pd.read_sql_query(
            """
            SELECT
              merchant_category,
              COUNT(*) AS fraud_attempts,
              COALESCE(SUM(amount), 0) AS fraud_amount
            FROM fraud_alerts
            WHERE event_time >= %s AND event_time < %s
            GROUP BY merchant_category
            ORDER BY fraud_attempts DESC, fraud_amount DESC;
            """,
            conn,
            params=[start_ts, end_ts],
        )

    stamp = end_ts.strftime("%Y%m%dT%H%M%SZ")
    recon_path = os.path.join(REPORTS_DIR, f"reconciliation_{stamp}.csv")
    fraud_cat_path = os.path.join(REPORTS_DIR, f"fraud_by_category_{stamp}.csv")

    recon_df.to_csv(recon_path, index=False)
    fraud_cat_df.to_csv(fraud_cat_path, index=False)

    print(f"[airflow] wrote: {recon_path}")
    print(f"[airflow] wrote: {fraud_cat_path}")


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="fintech_etl_validate_and_reports_every_6_hours",
    description="Every 6 hours: build validated set, store to Postgres + Parquet, then generate reconciliation + fraud-category reports",
    start_date=datetime(2026, 3, 16),
    schedule="0 */6 * * *",
    catchup=False,
    default_args=default_args,
    tags=["fintech", "etl", "warehouse", "reports"],
) as dag:

    t_validate = PythonOperator(
        task_id="build_validated_and_store",
        python_callable=build_validated_and_store,
    )

    t_reports = PythonOperator(
        task_id="generate_reports",
        python_callable=generate_reports,
    )

    t_validate >> t_reports