from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092"
)

producer.send(
    "transactions",
    b"test-transaction"
)

producer.flush()

print("Message sent")