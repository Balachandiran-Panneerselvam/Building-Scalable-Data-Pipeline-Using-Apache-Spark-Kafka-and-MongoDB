from confluent_kafka import Producer
import json

# Kafka producer configuration
producer = Producer({
    'bootstrap.servers': 'localhost:9092',  # Kafka broker
    'batch.num.messages': 1000,  # Number of messages to batch
    'linger.ms': 10,  # Delay to increase batch size
    'compression.type': 'snappy',  # Compress messages for efficiency
    'queue.buffering.max.messages': 100000  # Buffer size for messages
})


# Delivery report callback
def delivery_report(err, msg):
    """Delivery report callback to confirm message delivery."""
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


# JSONL file containing reviews
jsonl_file = "Digital_Music_data.jsonl"

# Batch size for sending messages
BATCH_SIZE = 10000 # Adjust batch size based on performance testing
batch = []  # Temporary storage for batched messages

# Stream reviews to Kafka in batches
with open(jsonl_file, 'r') as file:
    for line in file:
        review = json.loads(line)  # Parse each line as JSON
        batch.append((review.get('asin', 'default'), json.dumps(review)))

        # Check if the batch has reached the desired size
        if len(batch) >= BATCH_SIZE:
            print(f"Sending batch of {len(batch)} messages...")

            # Send each message in the batch
            for key, value in batch:
                producer.produce('amazon_reviews', key=key, value=value, callback=delivery_report)

            producer.flush()  # Ensure all messages in the batch are sent
            batch = []  # Clear the batch

# Send any remaining messages
if batch:
    print(f"Sending final batch of {len(batch)} messages...")
    for key, value in batch:
        producer.produce('amazon_reviews', key=key, value=value, callback=delivery_report)
    producer.flush()

print("All messages sent!")
