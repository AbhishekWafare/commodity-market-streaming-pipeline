from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'dead-letter-orders',
    bootstrap_servers='127.0.0.1:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for message in consumer:
    event = message.value

    print("\nINVALID ORDER RECEIVED!!")
    print(json.dumps(event, indent=4))


# DLQ pipeline

# Producer
#    ↓
# orders topic
#    ↓
# Main Consumer
#    ↓
# ┌───────────────────┐
# │ Valid Orders      │ → PostgreSQL
# └───────────────────┘

# ┌───────────────────┐
# │ Invalid Orders    │ → dead-letter-orders
# └───────────────────┘
#             ↓
#       DLQ Consumer
#             ↓
#     Monitoring / Alerting