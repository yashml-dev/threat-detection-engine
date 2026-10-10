
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    concat_ws,
    when,
    lit,
    coalesce,
    length,
    trim,
    window,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "transactions"

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
        .appName("FraudDetectionValidation")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0",
        )
        .getOrCreate()
    )


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        kafka_stream = (
            spark.readStream
            .format("kafka")
            .option(
                "kafka.bootstrap.servers",
                KAFKA_BOOTSTRAP_SERVERS,
            )
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", "earliest")
            .load()
        )

        # Step 1: Parse each Kafka message.
        parsed = (
            kafka_stream
            .select(
                col("topic"),
                col("partition"),
                col("offset"),
                col("value").cast("string").alias("raw_json"),
                from_json(
                    col("value").cast("string"),
                    TRANSACTION_SCHEMA,
                ).alias("tx"),
            )
            .select(
                "topic",
                "partition",
                "offset",
                "raw_json",
                "tx.*",
            )
        )

        # Step 2: Convert fields to useful processing types.
        checked = (
            parsed
            .withColumn(
                "amount_decimal",
                col("amount").cast(DecimalType(18, 2)),
            )
            .withColumn(
                "event_timestamp",
                to_timestamp(col("timestamp")),
            )
        )

        # Step 3: Collect validation errors.
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
                col("amount_decimal").isNull()
                | (col("amount_decimal") <= 0),
                lit("Amount must be numeric and greater than zero"),
            ),
            when(
                ~coalesce(col("currency").rlike("^[A-Z]{3}$"), lit(False)),
                lit("Invalid currency"),
            ),
            when(
                col("event_timestamp").isNull(),
                lit("Invalid timestamp"),
            ),
            when(
                ~coalesce(
                    col("payment_method").isin(
                        "card", "upi", "bank_transfer", "wallet"
                    ),
                    lit(False),
                ),
                lit("Invalid payment_method"),
            ),
            when(
                ~coalesce(
                    col("channel").isin("web", "mobile_app", "pos"),
                    lit(False),
                ),
                lit("Invalid channel"),
            ),
            when(
                ~coalesce(col("schema_version") == 1, lit(False)),
                lit("Unsupported schema_version"),
            ),
        )

        # A malformed JSON document produces a null transaction_id.
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
                when(
                    col("validation_errors") == "",
                    lit("VALID"),
                ).otherwise(lit("INVALID")),
            )
        )
        # Step 4: Detect high-value INR transactions.
        checked = (
            checked
            .withColumn(
                "high_amount_flag",
                when(
                    col("validation_status") == "VALID",
                    when(
                        (col("currency") == "INR")
                        & (col("amount_decimal") > 10000),
                        lit(True),
                    ).otherwise(lit(False)),
                ).otherwise(lit(None).cast("boolean")),
            )
            .withColumn(
                "risk_score",
                when(
                    col("validation_status") != "VALID",
                    lit(None).cast("integer"),
                ).when(
                    col("currency") != "INR",
                    lit(None).cast("integer"),
                ).when(
                    col("amount_decimal") > 30000,
                    lit(70),
                ).when(
                    col("amount_decimal") > 10000,
                    lit(50),

                ).otherwise(lit(0)),
            )
            .withColumn(
                "risk_level",
                when(
                    col("validation_status") != "VALID",
                    lit("DATA_QUALITY_REVIEW"),
                ).when(
                    col("currency") != "INR",
                    lit("NOT_SCORED"),
                ).when(
                    col("risk_score") >= 70,
                    lit("HIGH"),
                ).when(
                    col("risk_score") >= 30,
                    lit("MEDIUM"),
                ).otherwise(lit("LOW")),
            )
        )

                # Query 1: Display transaction risk assessments
        query = (
            checked
            .select(
                "transaction_id",
                "user_id",
                "merchant_id",
                "amount",
                "currency",
                "validation_status",
                "high_amount_flag",
                "risk_score",
                "risk_level",
                "validation_errors",
            )
            .writeStream
            .format("console")
            .outputMode("append")
            .option("truncate", "false")
            .option(
                "checkpointLocation",
                "checkpoints/transactions-risk-v1",
            )
            .trigger(processingTime="5 seconds")
            .start()
        )

        logger.info("Transaction risk assessment stream started.")

        # Query 2: Detect high transaction velocity
        velocity_counts = (
            checked
            .filter(
                (col("validation_status") == "VALID")
                & col("event_timestamp").isNotNull()
            )
            .withWatermark("event_timestamp", "10 seconds")
            .groupBy(
                window(
                    col("event_timestamp"),
                    "60 seconds",
                    "10 seconds",
                ),
                col("user_id"),
            )
            .count()
            .withColumnRenamed("count", "transaction_count")
            .filter(col("transaction_count") >= 5)
            .select(
                col("user_id"),
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("transaction_count"),
                lit("VELOCITY_ALERT").alias("alert_type"),
            )
        )

        velocity_query = (
            velocity_counts.writeStream
            .format("console")
            .outputMode("update")
            .option("truncate", "false")
            .option(
                "checkpointLocation",
                "checkpoints/velocity-detection-debug-v1",
            )
            .trigger(processingTime="5 seconds")
            .start()
        )

        logger.info("Velocity detection stream started.")
        query.awaitTermination()


    except KeyboardInterrupt:
        logger.info("Stopping transaction risk assessment stream.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

