import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, expr, window, countDistinct, lit, when, concat
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType


TOPIC = os.environ.get("KAFKA_TOPIC", "transactions")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")  # docker-internal
PG_URL = os.environ.get("PG_URL", "jdbc:postgresql://postgres:5432/fintech_db")
PG_USER = os.environ.get("PG_USER", "fintech")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "fintech")

HIGH_AMOUNT_THRESHOLD = float(os.environ.get("HIGH_AMOUNT_THRESHOLD", "5000"))


def build_spark():
    return (
        SparkSession.builder
        .appName("fintech-fraud-stream")
        # If running on spark container, master is usually set externally
        .getOrCreate()
    )


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("transaction_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("timestamp", StringType(), False),  # ISO string
        StructField("merchant_category", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("country", StringType(), False),
        StructField("city", StringType(), False),
    ])

    # 1) Read Kafka
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    # 2) Parse JSON
    parsed = (
        kafka_df
        .selectExpr("CAST(value AS STRING) AS value_str")
        .select(from_json(col("value_str"), schema).alias("j"))
        .select("j.*")
    )

    # 3) Convert timestamp -> event_time (event time)
    # Your producer outputs timestamps like: 2026-03-15T10:20:30.123Z
    # Spark can parse with pattern: yyyy-MM-dd'T'HH:mm:ss.SSSX
    enriched = (
        parsed
        .withColumn("event_time", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSX"))
        .drop("timestamp")
        .withWatermark("event_time", "10 minutes")
    )

    # ---- FRAUD RULES ----

    # Rule A: HIGH_AMOUNT
    high_amount = enriched.where(col("amount") > lit(HIGH_AMOUNT_THRESHOLD)) \
        .withColumn("fraud_type", lit("HIGH_AMOUNT")) \
        .withColumn("reason", concat(lit("Amount > "), lit(str(HIGH_AMOUNT_THRESHOLD))))

    # Rule B: IMPOSSIBLE_TRAVEL
    # Approach: in a 10-minute tumbling window per user, if countDistinct(country) >= 2, flag those events.
    windowed_country_counts = (
        enriched
        .groupBy(window(col("event_time"), "10 minutes"), col("user_id"))
        .agg(countDistinct(col("country")).alias("country_cnt"))
        .where(col("country_cnt") >= 2)
        .select(
            col("window.start").alias("w_start"),
            col("window.end").alias("w_end"),
            col("user_id")
        )
    )

    # Join back: mark events that fall into suspicious windows
    suspicious_events = (
        enriched.alias("e")
        .join(
            windowed_country_counts.alias("w"),
            (col("e.user_id") == col("w.user_id")) &
            (col("e.event_time") >= col("w.w_start")) &
            (col("e.event_time") < col("w.w_end")),
            "inner"
        )
        .drop("w_start", "w_end")
        .withColumn("fraud_type", lit("IMPOSSIBLE_TRAVEL"))
        .withColumn("reason", lit("Same user in >=2 countries within 10 minutes"))
    )

    # Union fraud events (dedupe by transaction_id)
    fraud_events = high_amount.select(enriched.columns + ["fraud_type", "reason"]) \
        .unionByName(suspicious_events.select(enriched.columns + ["fraud_type", "reason"])) \
        .dropDuplicates(["transaction_id"])

    # Validated = all events that are NOT fraud (anti-join by transaction_id)
    validated = enriched.alias("all").join(
        fraud_events.select("transaction_id").alias("f"),
        on="transaction_id",
        how="left_anti"
    )

    # ---- JDBC write helpers ----
    jdbc_props = {
        "user": PG_USER,
        "password": PG_PASSWORD,
        "driver": "org.postgresql.Driver",
    }

    def write_raw(batch_df, batch_id: int):
        (batch_df
         .select("transaction_id", "user_id", "event_time", "merchant_category", "amount", "country", "city")
         .write
         .jdbc(url=PG_URL, table="raw_transactions", mode="append", properties=jdbc_props))

    def write_validated(batch_df, batch_id: int):
        (batch_df
         .select("transaction_id", "user_id", "event_time", "merchant_category", "amount", "country", "city")
         .write
         .jdbc(url=PG_URL, table="validated_transactions", mode="append", properties=jdbc_props))

    def write_fraud(batch_df, batch_id: int):
        (batch_df
         .select("transaction_id", "user_id", "event_time", "fraud_type", "reason", "amount", "country", "city")
         .write
         .jdbc(url=PG_URL, table="fraud_alerts", mode="append", properties=jdbc_props))

    # ---- START STREAMS ----
    # NOTE: each foreachBatch is its own query
    q_raw = (
        enriched.writeStream
        .foreachBatch(write_raw)
        .outputMode("append")
        .option("checkpointLocation", "/tmp/checkpoints/raw")
        .start()
    )

    q_validated = (
        validated.writeStream
        .foreachBatch(write_validated)
        .outputMode("append")
        .option("checkpointLocation", "/tmp/checkpoints/validated")
        .start()
    )

    q_fraud = (
        fraud_events.writeStream
        .foreachBatch(write_fraud)
        .outputMode("append")
        .option("checkpointLocation", "/tmp/checkpoints/fraud")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()