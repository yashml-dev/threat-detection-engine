"""Idempotent PostgreSQL persistence for decisions and velocity alerts."""
import logging
import os
import psycopg

logger = logging.getLogger(__name__)

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "fraud_detection"),
    "user": os.getenv("POSTGRES_USER", "fraud_user"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

DECISION_INSERT_SQL = """
INSERT INTO fraud_transaction_decisions (
    topic, partition_id, kafka_offset, raw_json, transaction_id,
    user_id, merchant_id, amount, currency, validation_status,
    validation_errors, high_amount_flag, amount_risk_score,
    velocity_count, velocity_alert, final_risk_score, final_risk_level,
    event_timestamp, batch_id
) VALUES (
    %(topic)s, %(partition_id)s, %(kafka_offset)s, %(raw_json)s,
    %(transaction_id)s, %(user_id)s, %(merchant_id)s, %(amount)s,
    %(currency)s, %(validation_status)s, %(validation_errors)s,
    %(high_amount_flag)s, %(amount_risk_score)s, %(velocity_count)s,
    %(velocity_alert)s, %(final_risk_score)s, %(final_risk_level)s,
    %(event_timestamp)s, %(batch_id)s
)
ON CONFLICT DO NOTHING
"""

ALERT_UPSERT_SQL = """
INSERT INTO fraud_alerts (
    alert_type, user_id, window_start, window_end, transaction_count
) VALUES (
    %(alert_type)s, %(user_id)s, %(window_start)s, %(window_end)s,
    %(transaction_count)s
)
ON CONFLICT (alert_type, user_id)
DO UPDATE SET
    window_start = EXCLUDED.window_start,
    window_end = EXCLUDED.window_end,
    transaction_count = EXCLUDED.transaction_count
WHERE EXCLUDED.window_end >= fraud_alerts.window_end
"""


def save_batch_results(decisions, alerts):
    """Persist a micro-batch atomically; Kafka coordinates make replay idempotent."""
    if not decisions and not alerts:
        return
    if not DB_CONFIG["password"]:
        raise RuntimeError("Set POSTGRES_PASSWORD before starting the consumer.")

    with psycopg.connect(**DB_CONFIG) as connection:
        with connection.cursor() as cursor:
            if decisions:
                cursor.executemany(DECISION_INSERT_SQL, decisions)
                logger.info("Attempted persistence of %d transaction decisions.", len(decisions))
            if alerts:
                cursor.executemany(ALERT_UPSERT_SQL, alerts)
                logger.info("Processed %d velocity alert updates.", len(alerts))


def save_velocity_alerts(rows):
    """Compatibility wrapper for older code."""
    alerts = [
        {
            "alert_type": row["alert_type"],
            "user_id": row["user_id"],
            "window_start": row["window_start"],
            "window_end": row["window_end"],
            "transaction_count": row["transaction_count"],
        }
        for row in rows
    ]
    save_batch_results([], alerts)
