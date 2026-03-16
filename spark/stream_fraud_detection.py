import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    expr,
    lit,
    concat,
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType


TOPIC = (os.environ.get("KAFKA_TOPIC") or "transactions").strip()

# FIX: match docker-compose.core.yml container_name fintech-kafka
KAFKA_BOOTSTRAP = (os.environ.get("KAFKA_BOOTSTRAP") or "fintech-kafka:29092").strip()

# OK already: docker-compose.core.yml container_name fintech-postgres
PG_URL = (os.environ.get("PG_URL") or "jdbc:postgresql://fintech-postgres:5432/fintech_db").strip()
PG_USER = (os.environ.get("PG_USER") or "fintech").strip()
PG_PASSWORD = (os.environ.get("PG_PASSWORD") or "fintech").strip()

HIGH_AMOUNT_THRESHOLD = float(os.environ.get("HIGH_AMOUNT_THRESHOLD") or "5000")
CHECKPOINT_ROOT = (os.environ.get("CHECKPOINT_ROOT") or "/tmp/spark-checkpoints").strip()


def build_spark() -> SparkSession:
    return SparkSession.builder.appName("fintech-fraud-stream").getOrCreate()


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("transaction_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("timestamp", StringType(), False),
        StructField("merchant_category", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("country", StringType(), False),
        StructField("city", StringType(), False),
    ])

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = (
        kafka_df
        .selectExpr("CAST(value AS STRING) AS value_str")
        .select(from_json(col("value_str"), schema).alias("j"))
        .select("j.*")
    )

    enriched = (
        parsed
        .withColumn("event_time", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSX"))
        .drop("timestamp")
        .withWatermark("event_time", "10 minutes")
    )

    # Rule A: HIGH_AMOUNT
    high_amount = (
        enriched.where(col("amount") > lit(HIGH_AMOUNT_THRESHOLD))
        .withColumn("fraud_type", lit("HIGH_AMOUNT"))
        .withColumn("reason", concat(lit("Amount > "), lit(str(HIGH_AMOUNT_THRESHOLD))))
    )

    # Rule B: IMPOSSIBLE_TRAVEL
    a = enriched.alias("a")
    b = enriched.alias("b")

    impossible_travel_pairs = (
        a.join(
            b,
            (col("a.user_id") == col("b.user_id"))
            & (col("a.transaction_id") != col("b.transaction_id"))
            & (col("a.country") != col("b.country"))
            & (col("b.event_time") >= col("a.event_time"))
            & (col("b.event_time") <= expr("a.event_time + interval 10 minutes")),
            "inner",
        )
    )

    impossible_travel_events = (
        impossible_travel_pairs.selectExpr("a.*")
        .unionByName(impossible_travel_pairs.selectExpr("b.*"))
        .dropDuplicates(["transaction_id"])
        .withColumn("fraud_type", lit("IMPOSSIBLE_TRAVEL"))
        .withColumn("reason", lit("Same user in 2 countries within 10 minutes"))
    )

    fraud_events = (
        high_amount.select(enriched.columns + ["fraud_type", "reason"])
        .unionByName(impossible_travel_events.select(enriched.columns + ["fraud_type", "reason"]))
        .dropDuplicates(["transaction_id"])
    )

# #     # FIX: create validated stream = non-fraud
# #     fraud_ids = fraud_events.select("transaction_id").dropDuplicates(["transaction_id"])
# #     validated_events = enriched.join(fraud_ids, on="transaction_id", how="left_anti")

    jdbc_props = {
        "user": PG_USER,
        "password": PG_PASSWORD,
        "driver": "org.postgresql.Driver",
    }

    def write_raw(batch_df, batch_id: int):
        (
            batch_df
            .select("transaction_id", "user_id", "event_time", "merchant_category", "amount", "country", "city")
            .write
            .jdbc(url=PG_URL, table="raw_transactions", mode="append", properties=jdbc_props)
        )

    def write_fraud(batch_df, batch_id: int):
        (
            batch_df
            .select("transaction_id", "user_id", "event_time", "fraud_type", "reason", "amount", "country", "city")
            .write
            .jdbc(url=PG_URL, table="fraud_alerts", mode="append", properties=jdbc_props)
        )

    def write_validated(batch_df, batch_id: int):
        (
            batch_df
            .select("transaction_id", "user_id", "event_time", "merchant_category", "amount", "country", "city")
            .write
            .jdbc(url=PG_URL, table="validated_transactions", mode="append", properties=jdbc_props)
        )

    q_raw = (
        enriched.writeStream
        .foreachBatch(write_raw)
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/raw")
        .start()
    )

    q_fraud = (
        fraud_events.writeStream
        .foreachBatch(write_fraud)
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/fraud")
        .start()
    )

    # q_validated = (
    #     validated_events.writeStream
    #     .foreachBatch(write_validated)
    #     .outputMode("append")
    #     .option("checkpointLocation", f"{CHECKPOINT_ROOT}/validated")
    #     .start()
    # )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()