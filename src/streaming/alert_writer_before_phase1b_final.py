
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


def save_velocity_alerts(rows):
    """
    Persist a batch of Spark velocity alerts to PostgreSQL.

    Duplicate alerts are ignored using the table's unique constraint.
    """
    alerts = [
        (
            row["alert_type"],
            row["user_id"],
            row["window_start"],
            row["window_end"],
            row["transaction_count"],
        )
        for row in rows
    ]

    if not alerts:
        return

    if not DB_CONFIG["password"]:
        raise RuntimeError(
            "Set POSTGRES_PASSWORD before starting the consumer."
        )

    insert_sql = """
        INSERT INTO fraud_alerts (
            alert_type,
            user_id,
            window_start,
            window_end,
            transaction_count
        )
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (alert_type, user_id)
        DO UPDATE SET
            window_start = EXCLUDED.window_start,
            window_end = EXCLUDED.window_end,
            transaction_count = GREATEST(
                fraud_alerts.transaction_count,
                EXCLUDED.transaction_count
            )
        WHERE EXCLUDED.window_start >= fraud_alerts.window_start
    """

    with psycopg.connect(**DB_CONFIG) as connection:
        with connection.cursor() as cursor:
            cursor.executemany(insert_sql, alerts)
            inserted = cursor.rowcount

    logger.info(
        "Processed %d velocity alerts; inserted %d new records.",
        len(alerts),
        inserted,
    )
