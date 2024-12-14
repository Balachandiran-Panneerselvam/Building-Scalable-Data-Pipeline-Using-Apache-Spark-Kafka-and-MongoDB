from confluent_kafka import Consumer
from pymongo import MongoClient
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import json
import certifi  # Required for valid CA certificates

# MongoDB setup with valid CA certificate
mongo_client = MongoClient(
    "mongodb+srv://bala:dbamazonreviews@cluster0.xr0sb.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0",
    tls=True,
    tlsCAFile=certifi.where()  # Use certifi to ensure proper certificate validation
)

db = mongo_client["amazon_reviews_db"]
collection = db["processed_reviews"]

# Kafka consumer configuration
consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'amazon_reviews_group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(consumer_conf)
consumer.subscribe(['amazon_reviews'])

# Initialize VADER sentiment analyzer
analyzer = SentimentIntensityAnalyzer()

def analyze_sentiment_vader(text):
    """Analyze sentiment using VADER."""
    scores = analyzer.polarity_scores(text)
    if scores['compound'] >= 0.05:
        return "POSITIVE"
    elif scores['compound'] <= -0.05:
        return "NEGATIVE"
    else:
        return "NEUTRAL"

print("Waiting for messages...")

try:
    while True:
        msg = consumer.poll(1.0)  # Poll for new messages
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        # Deserialize the message
        review = json.loads(msg.value().decode('utf-8'))
        print(f"Received review: {review.get('title')}")

        # Perform sentiment analysis
        review_text = review.get("text", "")
        sentiment = analyze_sentiment_vader(review_text) if review_text else "UNKNOWN"

        # Prepare the review with sentiment
        processed_review = {
            "asin": review.get("asin"),
            "title": review.get("title"),
            "text": review_text,
            "rating": review.get("rating"),
            "sentiment": sentiment
        }

        # Save to MongoDB
        collection.insert_one(processed_review)
        print(f"Saved review with sentiment: {processed_review}")

except KeyboardInterrupt:
    print("Shutting down consumer...")
finally:
    consumer.close()
