import redis

client = redis.Redis(
    host="localhost",
    port=6379,
    decode_responses=True
)

client.set("test:key", "fraud-engine")

value = client.get("test:key")

print(value)