
from kafka import KafkaProducer
import json
import time
from datetime import datetime

from commodity_api_producer import get_api_data


producer = KafkaProducer(
    bootstrap_servers=['127.0.0.1:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    max_block_ms=15000,
    request_timeout_ms=15000,
    api_version_auto_timeout_ms=10000,
    acks='all'
)


def publish_event(topic, event):

    print(f"Publishing event to {topic}...", flush=True)

    future = producer.send(
        topic,
        value=event
    )

    metadata = future.get(timeout=20)

    print(
        f"Successfully delivered to {metadata.topic} | "
        f"Partition: {metadata.partition} | "
        f"Offset: {metadata.offset}",
        flush=True
    )


while True:

    try:
        print("\nFetching commodity prices...", flush=True)

        data = get_api_data()

        print("All commodity prices fetched successfully.", flush=True)

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # GOLD EVENT
        gold_event = {
            "commodity": "gold",
            "price": data["gold_price"],
            "currency": "USD",
            "timestamp": timestamp
        }

        publish_event('gold-prices', gold_event)

        # SILVER EVENT
        silver_event = {
            "commodity": "silver",
            "price": data["silver_price"],
            "currency": "USD",
            "timestamp": timestamp
        }

        publish_event('silver-prices', silver_event)

        # OIL EVENT
        oil_event = {
            "commodity": "oil",
            "price": data["oil_price"],
            "currency": "USD",
            "timestamp": timestamp
        }

        publish_event('oil-prices', oil_event)

        producer.flush(timeout=20)

        print("All three commodity events published successfully.", flush=True)
        print("Going to sleep for 300 seconds.", flush=True)

    except Exception as e:
        print("Producer Error:", repr(e), flush=True)

    time.sleep(300)
