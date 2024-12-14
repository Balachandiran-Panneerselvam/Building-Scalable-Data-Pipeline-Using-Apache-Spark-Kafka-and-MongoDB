# Building a Scalable Data Pipeline for Sentiment Analysis using PySpark, Kafka, MongoDB, Vader Sentiment Analyser

## **Summary**
This project implements a scalable data pipeline for real-time sentiment analysis using the Amazon Reviews dataset. It focuses on leveraging big data tools and technologies to process, analyze, and store customer reviews efficiently. The pipeline includes:

- **Data Ingestion**: A producer streams review data into an Apache Kafka topic.
- **Data Processing**: A consumer reads data from Kafka and processes it for analysis.
- **Data Storage**: Processed data is stored in MongoDB.
- **Sentiment Analysis**: Machine learning models are used to classify customer sentiment.

The pipeline is designed to handle large-scale datasets in real time and is extendable to other applications such as social media sentiment analysis or customer feedback monitoring.

---

## **Features**
- Real-time data ingestion using Apache Kafka.
- Distributed data processing for high-volume datasets.
- Scalable and flexible storage with MongoDB.
- Sentiment classification using machine learning models (e.g., Naïve Bayes, BERT).
- Extendable for diverse text-based datasets and applications.

---

## **Technologies Used**
- **Apache Kafka**: Real-time data streaming.
- **Apache Spark**: Distributed data processing.
- **MongoDB**: NoSQL storage for unstructured data.
- **Python**: Producer and consumer implementation.
- **Machine Learning Models**: For sentiment classification.

---

## **Dataset**
The dataset used is the Amazon Reviews Dataset. It contains:
- **Review Text**: Customer feedback.
- **Star Ratings**: Numeric ratings (1 to 5).
- **Product Metadata**: Additional product details.

Dataset URL: [Amazon Reviews Dataset](https://amazon-reviews-2023.github.io/)

---


## **Kafka Setup**
Download and install kafka in the local environment and start zookeeper, kafka and create kafka topic with partition and replication factor.

---

## **MongoDB Setup**
Create an account in MongoDB and create database with collection to store the data.
