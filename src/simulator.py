import json
import time
import random
import uuid
from kafka import KafkaProducer
import yaml

with open("configs/config.yaml") as f:
    config = yaml.safe_load(f)

producer = KafkaProducer(
    bootstrap_servers=config["kafka"]["bootstrap_servers"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

currencies = ["RUB", "USD", "EUR"]
sources = ["api_1", "api_2", "manual"]

while True:
    event = {
        "event_id": str(uuid.uuid4()),
        "currency": random.choice(currencies),
        "rate": round(random.uniform(70.0, 120.0), 4),
        "timestamp": int(time.time()),
        "source": random.choice(sources)
    }
    producer.send(config["kafka"]["topic"], value=event)
    print(f"Produced: {event['event_id']}")
    time.sleep(random.uniform(0.5, 2.0))
