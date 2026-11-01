from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'commodity-alerts',
    bootstrap_servers='127.0.0.1:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for message in consumer:
    alert = message.value

    print("\n🚨 PRICE ALERT RECEIVED 🚨")
    print(json.dumps(alert, indent=4))