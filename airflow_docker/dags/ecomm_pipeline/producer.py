import json
import re
from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField, StringSerializer
from utils.config import Config

TOPIC = 'kafka_ecommerce_stream'
producer = Producer({
    'bootstrap.servers': Config.BOOTSTRAP_SERVER,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': Config.CONFLUENT_API_KEY,
    'sasl.password': Config.CONFLUENT_API_SECRET,
    'client.id': 'ecommerce-producer-json'
})
serializer = StringSerializer('utf_8')

def is_valid_timestamp(timestamp):
    pattern = r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?([+-]\d{2}:\d{2}|Z)?$'
    return bool(re.match(pattern, timestamp))

class Event:
    def __init__(self, row):
        self.event_time = row.event_time
        self.event_type = row.event_type
        self.product_id = str(row.product_id)
        self.category_id = str(row.category_id)
        self.category_code = row.category_code
        self.brand = row.brand
        self.price = float(row.price)
        self.user_id = str(row.user_id)
        self.user_session = str(row.user_session)

    def to_dict(self):
        return {
            "event_time": self.event_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3],
            "event_type": self.event_type,
            "product_id": self.product_id,
            "category_id": self.category_id,
            "category_code": self.category_code,
            "brand": self.brand,
            "price": self.price,
            "user_id": self.user_id,
            "user_session": self.user_session
        }

def send_to_kafka(df):
    rows = df.collect()
    print(f"📦 Sending {len(rows)} records to Kafka...")

    for idx, row in enumerate(rows):
        try:
            event = Event(row)
            if not is_valid_timestamp(event.event_time.isoformat()):
                print(f"⛔ Invalid timestamp: {event.event_time}")
                continue

            producer.produce(
                topic=TOPIC,
                key=serializer(event.user_id, SerializationContext(TOPIC, MessageField.KEY)),
                value=serializer(json.dumps(event.to_dict()), SerializationContext(TOPIC, MessageField.VALUE))
            )
            if idx % 100 == 0:
                print(f"➡️ Sent {idx+1}/{len(rows)}")
        except Exception as e:
            print(f"❗Error at index {idx}: {e}")
        producer.poll(0)

    producer.flush()
    print(f"✅ Kafka flush complete.")
    print(f"📦 Sent {len(rows)} records to Kafka topic '{TOPIC}'")