from kafka import KafkaProducer
import json
import time
from datetime import datetime

from commodity_api_producer import get_api_data


producer = KafkaProducer(
    bootstrap_servers='127.0.0.1:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


while True:

    try:
        data = get_api_data()

        # GOLD EVENT
        gold_event = {
            "commodity": "gold",
            "price": data["gold_price"],
            "currency": "USD",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        producer.send(
            'gold-prices',
            value=gold_event
        )

        print("Gold Price Sent:", gold_event)


        # SILVER EVENT
        silver_event = {
            "commodity": "silver",
            "price": data["silver_price"],
            "currency": "USD",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        producer.send(
            'silver-prices',
            value=silver_event
        )

        print("Silver Price Sent:", silver_event)


        # OIL EVENT
        oil_event = {
            "commodity": "oil",
            "price": data["oil_price"],
            "currency": "USD",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        producer.send(
            'oil-prices',
            value=oil_event
        )

        print("Oil Price Sent:", oil_event)
        print("going Sleep for: 300 secs")

        producer.flush()

    except Exception as e:
        print("Producer Error:", e)

    time.sleep(300)