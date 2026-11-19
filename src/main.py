import json
import time
import yaml
from kafka import KafkaConsumer
from pydantic import BaseModel, ValidationError
import psycopg2
from psycopg2 import sql
from tenacity import retry, stop_after_attempt, wait_exponential

with open("configs/config.yaml") as f:
    config = yaml.safe_load(f)


class CurrencyEvent(BaseModel):
    event_id: str
    currency: str
    rate: float
    timestamp: int
    source: str


@retry(
    stop=stop_after_attempt(config["retry"]["max_attempts"]),
    wait=wait_exponential(
        multiplier=config["retry"]["wait_exponential_multiplier"],
        max=config["retry"]["wait_exponential_max"]
    )
)
def write_to_db(event: CurrencyEvent) -> None:
    conn = psycopg2.connect(
        host=config["postgres"]["host"],
        port=config["postgres"]["port"],
        dbname=config["postgres"]["dbname"],
        user=config["postgres"]["user"],
        password=config["postgres"]["password"]
    )
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS currency_stream (
                    event_id TEXT PRIMARY KEY,
                    currency TEXT,
                    rate DOUBLE PRECISION,
                    timestamp BIGINT,
                    source TEXT,
                    ingest_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cur.execute(
                sql.SQL("""
                    INSERT INTO currency_stream (event_id, currency, rate, timestamp, source)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (event_id) DO NOTHING
                """),
                (event.event_id, event.currency, event.rate, event.timestamp, event.source)
            )
        conn.commit()
    finally:
        conn.close()


def main():
    consumer = KafkaConsumer(
        config["kafka"]["topic"],
        bootstrap_servers=config["kafka"]["bootstrap_servers"],
        group_id=config["kafka"]["group_id"],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )

    for message in consumer:
        try:
            event = CurrencyEvent(**message.value)
            write_to_db(event)
            print(f"Processed: {event.event_id}")
        except ValidationError as e:
            print(f"Validation error: {e}")
        except Exception as e:
            print(f"Unexpected error: {e}")


if __name__ == "__main__":
    while True:
        try:
            main()
        except Exception as e:
            print(f"Consumer restart due to: {e}")
            time.sleep(5)
