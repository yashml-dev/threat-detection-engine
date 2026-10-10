
import json
import logging

from kafka import KafkaProducer
from kafka.errors import KafkaError

from src.ingestion.transaction_generator import stream_transactions

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "transactions"


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=3,
    )


def publish_transactions(count: int = 10) -> None:
    producer = create_producer()

    try:
        
        for index, transaction in enumerate(
            stream_transactions(
                interval_seconds=1.0,
                count=count,
            ),
            start=1,
        ):
            event = transaction.to_dict()

            # Test mode: first six transactions belong to one user.
            if index <= 6:
                event["user_id"] = "user_velocity_test"

            future = producer.send(KAFKA_TOPIC, value=event)
            metadata = future.get(timeout=10)

            logger.info(
                "Published transaction_id=%s | user_id=%s | topic=%s | offset=%s",
                event["transaction_id"],
                event["user_id"],
                metadata.topic,
                metadata.offset,
            )


    except KafkaError:
        logger.exception("Failed to publish transactions to Kafka.")
        raise

    finally:
        producer.close()


if __name__ == "__main__":
    publish_transactions(count=80)
