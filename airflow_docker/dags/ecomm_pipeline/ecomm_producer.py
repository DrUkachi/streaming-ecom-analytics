import os
import re
import json
from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField, StringSerializer
from pyspark.sql import SparkSession, functions as F, Window
from utils.config import Config

# Kafka Config
TOPIC = 'kafka_ecommerce_stream'
string_serializer = StringSerializer('utf_8')
producer = Producer({
    'bootstrap.servers': Config.BOOTSTRAP_SERVER,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': Config.CONFLUENT_API_KEY,
    'sasl.password': Config.CONFLUENT_API_SECRET,
    'client.id': 'ecommerce-producer-json'
})

CHECKPOINT_PATH = "checkpoint_ecommerce_rotating.json"
MONTHS_ORDERED = ["2019-Oct", "2019-Nov", "2019-Dec", "2020-Jan", "2020-Feb", "2020-Mar", "2020-Apr"]

def delivery_report(err, msg):
    if err:
        print(f"Delivery failed for message @ {msg.offset()}: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}")

def is_valid_timestamp(timestamp):
    pattern = r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?([+-]\d{2}:\d{2}|Z)?$'
    return bool(re.match(pattern, timestamp))

class Event:
    def __init__(self, row):
        self.event_time = row.event_time
        self.event_type = row.event_type
        self.product_id = str(row.product_id)
        self.category_id = str(row.category_id)
        self.category_code = row.category_code if row.category_code is not None else None
        self.brand = row.brand if row.brand is not None else None
        self.price = float(row.price) if row.price is not None else 0.0
        self.user_id = str(row.user_id)
        self.user_session = str(row.user_session)

    def to_dict(self):
        event_time_str = self.event_time.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]
        return {
            "event_time": event_time_str,
            "event_type": self.event_type,
            "product_id": self.product_id,
            "category_id": self.category_id,
            "category_code": self.category_code,
            "brand": self.brand,
            "price": self.price,
            "user_id": self.user_id,
            "user_session": self.user_session
        }

class RotatingStreamer:
    def __init__(self):
        self.spark = SparkSession.builder.appName("RotatingMonthlyStreamer").getOrCreate()
        self.checkpoint = self.load_checkpoint()
        self.current_month_index = MONTHS_ORDERED.index(self.checkpoint["current_month"])

    def load_checkpoint(self):
        try:
            with open(CHECKPOINT_PATH, "r") as f:
                data = json.load(f)
                assert "current_month" in data and "month_offset" in data
                if "global_offset" not in data:
                    data["global_offset"] = 100000
                return data
        except (FileNotFoundError, json.JSONDecodeError, AssertionError):
            return {
                "current_month": MONTHS_ORDERED[0],
                "month_offset": 0,
                "global_offset": 100000
            }

    def save_checkpoint(self):
        with open(CHECKPOINT_PATH, "w") as f:
            json.dump(self.checkpoint, f)

    def process_month_batch(self):
        month = self.checkpoint["current_month"]
        global_offset = self.checkpoint["global_offset"]

        print(f"🚀 Processing {month} | Global Offset: {global_offset}")

        # Load and prepare data
        df = self.spark.read.csv(f"/opt/data/{month}.csv", header=True)
        df = (
            df.withColumn("event_time", F.to_timestamp("event_time"))
            .withColumn("event_date", F.to_date("event_time"))
            .withColumn("product_id", F.col("product_id").cast("string"))
            .withColumn("category_id", F.col("category_id").cast("string"))
            .withColumn("user_id", F.col("user_id").cast("string"))
            .withColumn("user_session", F.col("user_session").cast("string"))
            .withColumn("brand", F.when(F.col("brand").isNotNull(), F.col("brand")).otherwise(None))
            .withColumn("category_code", F.when(F.col("category_code").isNotNull(), F.col("category_code")).otherwise(None))
            .withColumn("price", F.coalesce(F.col("price").cast("double"), F.lit(0.0)))
        )

        # Get all unique dates in the month, ordered
        dates = [row.event_date for row in df.select("event_date").distinct().orderBy("event_date").collect()]

        total_rows = 0
        for event_date in dates:
            print(f"Processing: {event_date}")

            # For each date, order by event_time and take up to 10,000 events
            daily_df = (
                df.filter(F.col("event_date") == event_date)
                .orderBy("event_time")
                .limit(10000)
            )

            count = daily_df.count()
            total_rows += count
            print(f"Loaded {count} events for {event_date}")

            rows = daily_df.collect()
            for idx, row in enumerate(rows):
                try:
                    event = Event(row)
                    if not is_valid_timestamp(event.event_time.isoformat()):
                        print(f"Skipping invalid timestamp: {event.event_time.isoformat()}")
                        continue

                    producer.produce(
                        topic=TOPIC,
                        key=string_serializer(event.user_id, SerializationContext(TOPIC, MessageField.KEY)),
                        value=string_serializer(
                            json.dumps(event.to_dict()),
                            SerializationContext(TOPIC, MessageField.VALUE)
                        ),
                        on_delivery=delivery_report
                    )
                    producer.poll(0)
                    if idx % 100 == 0:
                        print(f"[{idx:>4}] Event Time: {event.event_time.isoformat()}")
                except Exception as e:
                    print(f"\nError producing event {idx}: {e}")

            producer.flush()

        # Update checkpoints (global_offset is optional here since we process all dates in the month)
        self.checkpoint["global_offset"] += total_rows
        self.save_checkpoint()

        # Move to next month for next run
        self.current_month_index = (self.current_month_index + 1) % len(MONTHS_ORDERED)
        self.checkpoint["current_month"] = MONTHS_ORDERED[self.current_month_index]
        self.save_checkpoint()


if __name__ == "__main__":
    streamer = RotatingStreamer()
    streamer.process_month_batch()
    print(f"🔄 Next month: {streamer.checkpoint['current_month']}")
    print(f"📊 Global offset: {streamer.checkpoint['global_offset']}")
