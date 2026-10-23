-- Safe to re-run. Run as fraud_user against fraud_detection.
CREATE TABLE IF NOT EXISTS fraud_transaction_decisions (
    decision_id BIGSERIAL PRIMARY KEY,
    topic VARCHAR(249) NOT NULL,
    partition_id INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    raw_json TEXT,
    transaction_id VARCHAR(100) UNIQUE,
    user_id VARCHAR(100),
    merchant_id VARCHAR(100),
    amount NUMERIC(18, 2),
    currency VARCHAR(3),
    validation_status VARCHAR(20) NOT NULL,
    validation_errors TEXT NOT NULL DEFAULT '',
    high_amount_flag BOOLEAN,
    amount_risk_score INTEGER,
    velocity_count INTEGER,
    velocity_alert BOOLEAN NOT NULL DEFAULT FALSE,
    final_risk_score INTEGER,
    final_risk_level VARCHAR(30) NOT NULL,
    event_timestamp TIMESTAMPTZ,
    batch_id BIGINT,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT unique_kafka_record UNIQUE (topic, partition_id, kafka_offset)
);
CREATE INDEX IF NOT EXISTS idx_fraud_decisions_user_time
    ON fraud_transaction_decisions (user_id, event_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_fraud_decisions_final_level
    ON fraud_transaction_decisions (final_risk_level, processed_at DESC);

CREATE TABLE IF NOT EXISTS fraud_alerts (
    alert_id BIGSERIAL PRIMARY KEY,
    alert_type VARCHAR(50) NOT NULL,
    user_id VARCHAR(100) NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    transaction_count INTEGER NOT NULL,
    severity VARCHAR(20) NOT NULL DEFAULT 'HIGH',
    status VARCHAR(20) NOT NULL DEFAULT 'NEW',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT unique_velocity_alert UNIQUE (alert_type, user_id)
);
