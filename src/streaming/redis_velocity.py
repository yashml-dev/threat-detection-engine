
import time
import redis


class RedisVelocityTracker:
    def __init__(
        self,
        host="localhost",
        port=6379,
        window_seconds=60,
        threshold=5,
    ):
        self.client = redis.Redis(
            host=host,
            port=port,
            decode_responses=True,
        )
        self.window_seconds = window_seconds
        self.threshold = threshold

        # Redis executes this script atomically.
        self._record_script = self.client.register_script("""
            local key = KEYS[1]
            local txn_id = ARGV[1]
            local event_time = tonumber(ARGV[2])
            local window_seconds = tonumber(ARGV[3])
            local threshold = tonumber(ARGV[4])

            local cutoff = event_time - window_seconds

            -- Remove events outside the rolling window.
            redis.call(
                'ZREMRANGEBYSCORE', key, '-inf', cutoff
            )

            -- Add only if this transaction ID is not already present.
            redis.call(
                'ZADD', key, 'NX', event_time, txn_id
            )

            local count = redis.call('ZCARD', key)

            -- Keep the key alive while it contains recent events.
            redis.call('EXPIRE', key, window_seconds + 10)

            return count
        """)

    def record_transaction(
        self,
        user_id,
        transaction_id,
        timestamp=None,
    ):
        if not user_id or not transaction_id:
            raise ValueError(
                "user_id and transaction_id are required"
            )

        if timestamp is None:
            timestamp = time.time()

        key = f"velocity:{user_id}"

        count = self._record_script(
            keys=[key],
            args=[
                transaction_id,
                timestamp,
                self.window_seconds,
                self.threshold,
            ],
        )

        count = int(count)

        return {
            "user_id": user_id,
            "transaction_count": count,
            "velocity_alert": count >= self.threshold,
        }
