from confluent_kafka import Consumer, KafkaException
import json

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my-group',
    'auto.offset.reset': 'earliest',  # or 'latest'
}

consumer = Consumer(conf)
consumer.subscribe([
    'page-view',
    'product-click',
    'add-to-remove-from-cart',
    'purchase',
    'login-logout'
])

try:
    while True:
        msg = consumer.poll(1.0)  # timeout in seconds
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        else:
            data = json.loads(msg.value().decode('utf-8'))
            print(f"✅ Message received: {data}")

except Exception as e:
    print(f"Error: {e}")
finally:
    consumer.close()