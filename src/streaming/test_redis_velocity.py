
from src.streaming.redis_velocity import RedisVelocityTracker

tracker = RedisVelocityTracker(
    window_seconds=60,
    threshold=5,
)

user_id = "redis_test_suite"

# Test 1: Five unique transactions should trigger an alert.
print("\nTest 1: Velocity threshold")

for i in range(1, 6):
    result = tracker.record_transaction(
        user_id=user_id,
        transaction_id=f"txn_{i}",
    )
    print(result)

assert result["transaction_count"] == 5
assert result["velocity_alert"] is True
print("PASS: Alert triggered at five transactions.")


# Test 2: A duplicate transaction must not increase the count.
print("\nTest 2: Duplicate protection")

duplicate_user = "redis_duplicate_suite"

first = tracker.record_transaction(
    user_id=duplicate_user,
    transaction_id="txn_duplicate_001",
)

second = tracker.record_transaction(
    user_id=duplicate_user,
    transaction_id="txn_duplicate_001",
)

print("First call:", first)
print("Duplicate call:", second)

assert first["transaction_count"] == 1
assert second["transaction_count"] == 1
print("PASS: Duplicate transaction was not counted twice.")
print("\nTest 3: Rolling-window expiry")

window_user = "redis_window_suite"
base_time = 1_800_000_000.0

tracker.record_transaction(
    user_id=window_user,
    transaction_id="old_txn",
    timestamp=base_time,
)

result = tracker.record_transaction(
    user_id=window_user,
    transaction_id="new_txn",
    timestamp=base_time + 61,
)

print(result)

assert result["transaction_count"] == 1
assert result["velocity_alert"] is False
print("PASS: Expired transaction was excluded.")
