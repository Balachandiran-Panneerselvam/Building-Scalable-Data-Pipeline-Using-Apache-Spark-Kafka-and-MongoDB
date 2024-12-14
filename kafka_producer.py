from confluent_kafka import Producer
import json
import time

# Kafka producer configuration
producer = Producer({'bootstrap.servers': 'localhost:9092'})

# Delivery report callback
def delivery_report(err, msg):
    """Delivery report callback."""
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# JSONL file containing reviews
jsonl_file = "Arts_Crafts_and_Sewing.jsonl"

# Stream reviews to Kafka
with open(jsonl_file, 'r') as file:
    for line in file:
        review = json.loads(line)  # Parse each line as JSON
        producer.produce('amazon_reviews', key=review.get('asin', 'default'), value=json.dumps(review), callback=delivery_report)
        print(f"Sent review: {review['title']}")
        time.sleep(1)  # Simulate streaming delay

# Wait for all messages to be delivered
producer.flush()

