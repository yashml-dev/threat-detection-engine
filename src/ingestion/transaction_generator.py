
import random
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Iterator

from src.ingestion.transaction_schema import Transaction


PAYMENT_METHODS = ["card", "upi", "bank_transfer", "wallet"]
CHANNELS = ["web", "mobile_app", "pos"]
COUNTRIES = ["IN", "IN", "IN", "US", "GB"]

MERCHANTS = ["merchant_51", "merchant_52", "merchant_53"]
USERS = [f"user_{i:03d}" for i in range(1, 101)]
DEVICES = [f"device_{i:03d}" for i in range(1, 81)]


def generate_transaction() -> Transaction:
    """Generate one valid synthetic transaction."""

    amount = Decimal(
        random.randint(100, 5_000_000)
    ) / Decimal("100")

    return Transaction(
        transaction_id=f"txn_{uuid.uuid4().hex[:12]}",
        user_id=random.choice(USERS),
        merchant_id=random.choice(MERCHANTS),
        amount=amount,
        currency="INR",
        timestamp=datetime.now(timezone.utc),
        payment_method=random.choice(PAYMENT_METHODS),
        channel=random.choice(CHANNELS),
        device_id=random.choice(DEVICES),
        ip_address=f"192.0.2.{random.randint(1, 254)}",
        location_country=random.choice(COUNTRIES),
    )


def stream_transactions(
    interval_seconds: float = 1.0,
    count: int | None = None,
) -> Iterator[Transaction]:
    """Generate transactions continuously, or stop after count events."""

    if interval_seconds < 0:
        raise ValueError("interval_seconds cannot be negative")

    generated = 0

    while count is None or generated < count:
        yield generate_transaction()
        generated += 1

        if count is None or generated < count:
            time.sleep(interval_seconds)


if __name__ == "__main__":
    for transaction in stream_transactions(
        interval_seconds=1.0,
        count=10,
    ):
        print(transaction.to_json())
