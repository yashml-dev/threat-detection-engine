-- Run after the producer finishes.
SELECT transaction_id, user_id, amount_risk_score, velocity_count,
       velocity_alert, final_risk_score, final_risk_level, event_timestamp
FROM fraud_transaction_decisions
WHERE user_id = 'user_velocity_test'
  AND processed_at >= NOW() - INTERVAL '15 minutes'
ORDER BY event_timestamp;

SELECT alert_type, user_id, transaction_count, window_start, window_end, status
FROM fraud_alerts
WHERE user_id = 'user_velocity_test'
  AND alert_type = 'VELOCITY_ALERT';

SELECT COUNT(*) AS recent_decisions,
       COUNT(*) FILTER (WHERE velocity_alert) AS velocity_flagged_decisions,
       MAX(final_risk_score) AS highest_final_score
FROM fraud_transaction_decisions
WHERE user_id = 'user_velocity_test'
  AND processed_at >= NOW() - INTERVAL '15 minutes';
