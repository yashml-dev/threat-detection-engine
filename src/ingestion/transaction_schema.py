
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any
import json
import re


@dataclass
class Transaction:
    transaction_id: str
    user_id: str
    merchant_id: str
    amount: Decimal
    currency: str
    timestamp: datetime
    payment_method: str
    channel: str
    device_id: str | None = None
    ip_address: str | None = None
    location_country: str | None = None
    schema_version: int = 1

    def __post_init__(self):
        # Required identifiers
        for field_name in (
            "transaction_id",
            "user_id",
            "merchant_id",
        ):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field_name} must be a non-empty string")

        # Keep financial values precise; never use float for money.
        try:
            self.amount = Decimal(str(self.amount))
        except (InvalidOperation, ValueError):
            raise ValueError("amount must be a valid decimal number")

        if not self.amount.is_finite() or self.amount <= 0:
            raise ValueError("amount must be finite and greater than zero")

        # Currency: ISO-style three-letter code.
        if not re.fullmatch(r"[A-Z]{3}", self.currency):
            raise ValueError("currency must be a 3-letter uppercase code")

        # Timestamp must include a timezone.
        if not isinstance(self.timestamp, datetime):
            raise ValueError("timestamp must be a datetime object")

        if self.timestamp.tzinfo is None:
            raise ValueError("timestamp must include a timezone")

        self.timestamp = self.timestamp.astimezone(timezone.utc)

        # Restrict values to our initial supported categories.
        allowed_methods = {
            "card", "upi", "bank_transfer", "wallet"
        }
        allowed_channels = {
            "web", "mobile_app", "pos"
        }

        if self.payment_method not in allowed_methods:
            raise ValueError("Unsupported payment_method")

        if self.channel not in allowed_channels:
            raise ValueError("Unsupported channel")

        if not isinstance(self.schema_version, int) or self.schema_version != 1:
            raise ValueError("schema_version must be 1")

        for field_name in (
            "device_id", "ip_address", "location_country"
        ):
            value = getattr(self, field_name)
            if value is not None and (
                not isinstance(value, str) or not value.strip()
            ):
                raise ValueError(f"{field_name} must be a non-empty string or None")

        if self.location_country is not None:
            self.location_country = self.location_country.upper()
            if not re.fullmatch(r"[A-Z]{2}", self.location_country):
                raise ValueError("location_country must be a 2-letter country code")

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Transaction":
        """Create a validated transaction from decoded JSON data."""
        required_fields = {
            "transaction_id",
            "user_id",
            "merchant_id",
            "amount",
            "currency",
            "timestamp",
            "payment_method",
            "channel",
        }

        missing = required_fields - data.keys()
        if missing:
            raise ValueError(f"Missing required fields: {sorted(missing)}")

        allowed_fields = {
            "transaction_id", "user_id", "merchant_id",
            "amount", "currency", "timestamp", "payment_method",
            "channel", "device_id", "ip_address", "location_country",
            "schema_version",
        }

        unknown = data.keys() - allowed_fields
        if unknown:
            raise ValueError(f"Unknown fields: {sorted(unknown)}")

        try:
            timestamp = datetime.fromisoformat(
                data["timestamp"].replace("Z", "+00:00")
            )
        except (ValueError, AttributeError, TypeError):
            raise ValueError("timestamp must be a valid ISO 8601 datetime")

        return cls(
            **{
                **data,
                "timestamp": timestamp,
            }
        )

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible transaction event."""
        return {
            "schema_version": self.schema_version,
            "transaction_id": self.transaction_id,
            "user_id": self.user_id,
            "merchant_id": self.merchant_id,
            "amount": format(self.amount, "f"),
            "currency": self.currency,
            "timestamp": (
                self.timestamp.isoformat()
                .replace("+00:00", "Z")
            ),
            "payment_method": self.payment_method,
            "channel": self.channel,
            "device_id": self.device_id,
            "ip_address": self.ip_address,
            "location_country": self.location_country,
        }

    def to_json(self) -> str:
        """Serialize the event for Kafka."""
        return json.dumps(self.to_dict())

if __name__ == "__main__":
    sample = {
        "schema_version": 1,
        "transaction_id": "txn_100001",
        "user_id": "user_204",
        "merchant_id": "merchant_51",
        "amount": "1250.50",
        "currency": "INR",
        "timestamp": "2026-10-09T15:30:00Z",
        "payment_method": "upi",
        "channel": "mobile_app",
        "device_id": "device_88",
        "ip_address": "192.0.2.10",
        "location_country": "IN",
    }

    transaction = Transaction.from_dict(sample)

    print("Validated transaction:")
    print(transaction.to_json())

    print("\nTransaction amount:", transaction.amount)
    print("Transaction timestamp:", transaction.timestamp)
