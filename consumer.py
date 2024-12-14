from confluent_kafka import Consumer, TopicPartition
from pymongo import MongoClient
from pyspark.sql.functions import col
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import json
import certifi  # Required for valid CA certificates
import matplotlib.pyplot as plt
# MongoDB setup with valid CA certificate
mongo_client = MongoClient(
    "YOUR MONGO CLIENT CONNECTION STRING",
    tls=True,
    tlsCAFile=certifi.where()  # Use certifi to ensure proper certificate validation
)

db = mongo_client["amazon_reviews_db"]
collection = db["processed_reviews"]

# Kafka consumer configuration
consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'amazon_reviews_group',
    'enable.auto.commit': False,  # Disable auto-commit to control offsets manually
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

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Amazon Reviews Processor") \
    .getOrCreate()

# Define schema for PySpark DataFrame
schema = StructType([
    StructField("asin", StringType(), True),
    StructField("title", StringType(), True),
    StructField("text", StringType(), True),
    StructField("rating", StringType(), True),
    StructField("sentiment", StringType(), True)
])

# Initialize an empty DataFrame for accumulating processed data
final_df = spark.createDataFrame([], schema)

print("Waiting for messages...")

BATCH_SIZE = 10000  # Process messages in batches
CONSECUTIVE_EMPTY_POLLS = 10  # Number of empty polls before exiting

try:
    consecutive_empty_polls = 0
    while True:
        batch = []  # Temporary storage for the batch
        offsets = []  # Track offsets for manual commit

        # Fetch messages until the batch size is reached or no more messages are available
        while len(batch) < BATCH_SIZE:
            msg = consumer.poll(1.0)  # Poll for new messages
            if msg is None:
                consecutive_empty_polls += 1
                if consecutive_empty_polls >= CONSECUTIVE_EMPTY_POLLS:
                    if batch:
                        print(f"Processing final batch of {len(batch)} messages...")
                        # Save final batch to MongoDB and accumulate in the DataFrame
                        processed_batch = [
                            {
                                "asin": review.get("asin"),
                                "title": review.get("title"),
                                "text": review.get("text", ""),
                                "rating": review.get("rating"),
                                "sentiment": analyze_sentiment_vader(review.get("text", ""))
                            }
                            for review in batch
                        ]

                        # Save to MongoDB
                        collection.insert_many(processed_batch)

                        # Convert to PySpark DataFrame
                        batch_df = spark.createDataFrame(processed_batch, schema=schema)
                        final_df = final_df.union(batch_df)  # Append to the cumulative DataFrame
                    print("No more messages. Exiting.")
                    raise KeyboardInterrupt  # Exit the outer loop cleanly
                else:
                    continue
            else:
                consecutive_empty_polls = 0  # Reset empty poll count
                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue

                # Deserialize the message
                review = json.loads(msg.value().decode('utf-8'))
                batch.append(review)
                offsets.append(TopicPartition(msg.topic(), msg.partition(), msg.offset() + 1))

        if not batch:
            continue

        print(f"Processing batch of {len(batch)} messages...")

        # Process each message in the batch
        processed_batch = []
        for review in batch:
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
            processed_batch.append(processed_review)

        # Save the batch to MongoDB
        if processed_batch:
            collection.insert_many(processed_batch)
            print(f"Saved batch of {len(processed_batch)} reviews to MongoDB.")

            # Convert to PySpark DataFrame
            batch_df = spark.createDataFrame(processed_batch, schema=schema)
            final_df = final_df.union(batch_df)  # Append to the cumulative DataFrame

        # Manually commit offsets after processing the batch
        consumer.commit(offsets=offsets)

except KeyboardInterrupt:
    print("Shutting down consumer...")
    print("Final DataFrame:")
    #final_df.show(truncate=False)  # Show the final DataFrame
finally:
    consumer.close()

# Group by sentiment and count occurrences
sentiment_grouped = final_df.filter(col("sentiment") != "UNKNOWN").groupBy("sentiment").count()
sentiment_df = sentiment_grouped.toPandas()  # Convert to Pandas for visualization

# Group by rating and count occurrences
rating_grouped = final_df.groupBy("rating").count()
rating_df = rating_grouped.toPandas().sort_values(by="rating")  # Convert to Pandas for visualization

# Pie chart for sentiment distribution
plt.figure(figsize=(8, 8))
plt.pie(sentiment_df['count'], labels=sentiment_df['sentiment'], autopct='%1.1f%%', startangle=90, colors=plt.cm.Paired.colors)
plt.title('Sentiment Distribution')
plt.show()

# Bar chart for rating distribution
plt.figure(figsize=(8, 6))
plt.bar(rating_df['rating'].astype(str), rating_df['count'], color='skyblue', edgecolor='black')
plt.xlabel('Ratings')
plt.ylabel('Count')
plt.title('Rating Distribution')
plt.show()



spark.stop()