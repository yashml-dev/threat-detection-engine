import logging
from datetime import timedelta, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, concat_ws, when, lit, coalesce, length, trim,
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DecimalType,
)

try:
    from .alert_writer import save_batch_results
    from .redis_velocity import RedisVelocityTracker
    from .risk_scoring import calculate_unified_risk
except ImportError:
    # Supports: python src/streaming/consumer.py
    from alert_writer import save_batch_results
    from redis_velocity import RedisVelocityTracker
    from risk_scoring import calculate_unified_risk

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "transactions"
VELOCITY_WINDOW_SECONDS = 60
VELOCITY_THRESHOLD = 5

TRANSACTION_SCHEMA = StructType([
    StructField("schema_version", IntegerType(), True),
    StructField("transaction_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("amount", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("location_country", StringType(), True),
])


def create_spark_session():
    return (
        SparkSession.builder
        .appName("UnifiedFraudRiskEngine")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0,"
            "org.postgresql:postgresql:42.7.7",
        )
        .getOrCreate()
    )


def _epoch_seconds(event_time):
    if event_time.tzinfo is None:
        event_time = event_time.replace(tzinfo=timezone.utc)
    return event_time.timestamp()


def persist_unified_batch(batch_df, batch_id, tracker):
    """Calculate velocity and persist unified decisions for one micro-batch."""
    rows = (
        batch_df
        .select(
            "topic", "partition", "offset", "raw_json",
            "transaction_id", "user_id", "merchant_id", "amount_decimal",
            "currency", "validation_status", "validation_errors",
            "high_amount_flag", "event_timestamp", "risk_score",
        )
        .orderBy(col("event_timestamp").asc_nulls_last(), col("partition"), col("offset"))
        .collect()
    )

    decisions = []
    alerts = []

    for row in rows:
        event_time = row["event_timestamp"]
        status = row["validation_status"]
        transaction_id = row["transaction_id"]
        user_id = row["user_id"]
        velocity_count = None
        velocity_alert = False

        if status == "VALID" and event_time is not None and transaction_id and user_id:
            velocity = tracker.record_transaction(
                user_id=user_id,
                transaction_id=transaction_id,
                timestamp=_epoch_seconds(event_time),
            )
            velocity_count = velocity["transaction_count"]
            velocity_alert = velocity["velocity_alert"]

        amount_score = row["risk_score"]
        if status != "VALID":
            final_score, final_level = None, "DATA_QUALITY_REVIEW"
        elif row["currency"] != "INR":
            final_score, final_level = None, "NOT_SCORED"
        else:
            final_score, final_level = calculate_unified_risk(
                amount_score, velocity_alert
            )

        decisions.append({
            "topic": row["topic"],
            "partition_id": int(row["partition"]),
            "kafka_offset": int(row["offset"]),
            "raw_json": row["raw_json"],
            "transaction_id": transaction_id,
            "user_id": user_id,
            "merchant_id": row["merchant_id"],
            "amount": row["amount_decimal"],
            "currency": row["currency"],
            "validation_status": status,
            "validation_errors": row["validation_errors"] or "",
            "high_amount_flag": row["high_amount_flag"],
            "amount_risk_score": amount_score,
            "velocity_count": velocity_count,
            "velocity_alert": velocity_alert,
            "final_risk_score": final_score,
            "final_risk_level": final_level,
            "event_timestamp": event_time,
            "batch_id": int(batch_id),
        })

        if velocity_alert and event_time is not None:
            alerts.append({
                "alert_type": "VELOCITY_ALERT",
                "user_id": user_id,
                "window_start": event_time - timedelta(seconds=VELOCITY_WINDOW_SECONDS),
                "window_end": event_time,
                "transaction_count": velocity_count,
            })

    if decisions or alerts:
        save_batch_results(decisions, alerts)

    for decision in decisions:
        logger.info(
            "UNIFIED_RISK batch_id=%s transaction_id=%s user_id=%s "
            "status=%s amount_score=%s velocity_count=%s "
            "velocity_alert=%s final_score=%s final_level=%s",
            batch_id, decision["transaction_id"], decision["user_id"],
            decision["validation_status"], decision["amount_risk_score"],
            decision["velocity_count"], decision["velocity_alert"],
            decision["final_risk_score"], decision["final_risk_level"],
        )

    logger.info(
        "Batch %s completed: %d decisions, %d velocity alerts.",
        batch_id, len(decisions), len(alerts),
    )


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    tracker = RedisVelocityTracker(
        host="localhost", port=6379,
        window_seconds=VELOCITY_WINDOW_SECONDS,
        threshold=VELOCITY_THRESHOLD,
    )

    try:
        tracker.client.ping()
        logger.info("Redis connection verified.")

        kafka_stream = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", "latest")
            .load()
        )

        parsed = (
            kafka_stream
            .select(
                col("topic"), col("partition"), col("offset"),
                col("value").cast("string").alias("raw_json"),
                from_json(col("value").cast("string"), TRANSACTION_SCHEMA).alias("tx"),
            )
            .select("topic", "partition", "offset", "raw_json", "tx.*")
        )

        checked = (
            parsed
            .withColumn("amount_decimal", col("amount").cast(DecimalType(18, 2)))
            .withColumn("event_timestamp", to_timestamp(col("timestamp")))
        )

        errors = concat_ws(
            "; ",
            when(
                col("transaction_id").isNull()
                | (length(trim(coalesce(col("transaction_id"), lit("")))) == 0),
                lit("Missing transaction_id"),
            ),
            when(
                col("user_id").isNull()
                | (length(trim(coalesce(col("user_id"), lit("")))) == 0),
                lit("Missing user_id"),
            ),
            when(
                col("merchant_id").isNull()
                | (length(trim(coalesce(col("merchant_id"), lit("")))) == 0),
                lit("Missing merchant_id"),
            ),
            when(
                col("amount_decimal").isNull() | (col("amount_decimal") <= 0),
                lit("Amount must be numeric and greater than zero"),
            ),
            when(
                ~coalesce(col("currency").rlike("^[A-Z]{3}$"), lit(False)),
                lit("Invalid currency"),
            ),
            when(col("event_timestamp").isNull(), lit("Invalid timestamp")),
            when(
                ~coalesce(
                    col("payment_method").isin("card", "upi", "bank_transfer", "wallet"),
                    lit(False),
                ),
                lit("Invalid payment_method"),
            ),
            when(
                ~coalesce(col("channel").isin("web", "mobile_app", "pos"), lit(False)),
                lit("Invalid channel"),
            ),
            when(
                ~coalesce(col("schema_version") == 1, lit(False)),
                lit("Unsupported schema_version"),
            ),
        )

        checked = (
            checked
            .withColumn(
                "validation_errors",
                when(
                    col("transaction_id").isNull()
                    & col("user_id").isNull()
                    & col("merchant_id").isNull()
                    & col("amount").isNull(),
                    lit("Malformed JSON or missing transaction fields"),
                ).otherwise(errors),
            )
            .withColumn(
                "validation_status",
                when(col("validation_errors") == "", lit("VALID"))
                .otherwise(lit("INVALID")),
            )
            .withColumn(
                "high_amount_flag",
                when(
                    col("validation_status") == "VALID",
                    when(
                        (col("currency") == "INR") & (col("amount_decimal") > 10000),
                        lit(True),
                    ).otherwise(lit(False)),
                ).otherwise(lit(None).cast("boolean")),
            )
            .withColumn(
                "risk_score",
                when(col("validation_status") != "VALID", lit(None).cast("integer"))
                .when(col("currency") != "INR", lit(None).cast("integer"))
                .when(col("amount_decimal") > 30000, lit(70))
                .when(col("amount_decimal") > 10000, lit(50))
                .otherwise(lit(0)),
            )
        )

        query = (
            checked.writeStream
            .foreachBatch(lambda df, batch_id: persist_unified_batch(df, batch_id, tracker))
            .option("checkpointLocation", "checkpoints/unified-risk-v1")
            .trigger(processingTime="5 seconds")
            .start()
        )
        logger.info(
            "Unified risk stream started: topic=%s threshold=%d window=%ds",
            KAFKA_TOPIC, VELOCITY_THRESHOLD, VELOCITY_WINDOW_SECONDS,
        )
        query.awaitTermination()

    except KeyboardInterrupt:
        logger.info("Stopping unified fraud risk stream.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
