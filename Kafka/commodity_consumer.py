from kafka import KafkaConsumer, KafkaProducer
import json
from datetime import datetime



import psycopg2
import os

# from dotenv import load_dotenv
# load_dotenv()

def get_connection():

    return psycopg2.connect(
        # host=os.getenv("POSTGRES_HOST", "localhost"),
        host=os.getenv("POSTGRES_HOST", "127.0.0.1"),
        database=os.getenv("POSTGRES_DB", "kafka_db"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5434"))
    )

conn = get_connection()
cursor = conn.cursor()

# Dead Letter Queue (DLQ)
dlq_producer = KafkaProducer(
    bootstrap_servers='127.0.0.1:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

consumer = KafkaConsumer(
    'gold-prices',
    'silver-prices',
    'oil-prices',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='commodity-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)
def send_alert(commodity, price, message):

    alert_event = {
        "alert_for": commodity,
        "price": price,
        "message": message,
        "alert_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    dlq_producer.send(
        'commodity-alerts',
        value=alert_event
    )

    dlq_producer.flush()

    print("Sent to commodity-alerts:", alert_event)

# analytics storage
commodity_stats = {}

try:
    for message in consumer:

        try:
            # validate event, insert into db
            event = message.value

            commodity = event.get("commodity", "unknown")
            price = event.get("price", 0)
            currency = event.get("currency", "INR")
            timestamp = event.get("timestamp", "")

            if price <= 0:
                raise ValueError("Invalid amount")
            # alert rules
            if commodity == "gold" and price > 5000:
                send_alert(
                    commodity,
                    price,
                    f"{commodity} price crossed {price}"
                )

            if commodity == "silver" and price > 80:
                send_alert(
                    commodity,
                    price,
                    f"{commodity} price fell to {price}"
                )

            if commodity == "oil" and price > 100:
                send_alert(
                    commodity,
                    price,
                    f"{commodity} price crossed {price}"
                )

            # first event setup
            if commodity not in commodity_stats:
                commodity_stats[commodity] = {
                    "count": 0,
                    "total": 0,
                    "highest": price,
                    "lowest": price
                }

            # update stats
            stats = commodity_stats[commodity]

            stats["count"] += 1
            stats["total"] += price

            if price > stats["highest"]:
                stats["highest"] = price

            if price < stats["lowest"]:
                stats["lowest"] = price

            average_price = stats["total"] / stats["count"]

            print("=" * 50)

            print(f"\n---- Commodity: {commodity.upper()} ----")
            print(f"Latest Price: ₹{price}")
            print(f"Currency: {currency}")
            print(f"Timestamp: {timestamp}")

            print("\n---- Analytics ----")
            print(f"Total Events: {stats['count']}")
            print(f"Highest Price: ₹{stats['highest']}")
            print(f"Lowest Price: ₹{stats['lowest']}")
            print(f"Average Price: ₹{average_price:.2f}")

            cursor.execute(
                    """
                    INSERT INTO commodity_prices (commodity, price, currency, timestamp)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (commodity, price, currency, timestamp)
                )

            conn.commit()

        except Exception as e:
            # send to dead-letter-events
            conn.rollback()
            print("ERROR:", e)
            dlq_event = {
                "error": str(e),
                "failed_event": event,
                "failed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            dlq_producer.send(
                'dead-letter-prices',
                value=dlq_event
            )

            print("Sent to DLQ:", event)
            dlq_producer.flush()
    print('------------------------------------------------------------------------------------------------------------------')
finally:
    cursor.close()
    conn.close()
