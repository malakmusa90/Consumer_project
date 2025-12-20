Spark Kafka Consumer with MongoDB + Bloom Filter

This project reads streaming news messages from Apache Kafka, applies a Bloom Filter to remove duplicates, cleans text , and finally stores the processed output in MongoDB.

It is designed to simulate a real-time news pipeline.

--------------------------------------------------------
Tech Stack

Scala

Apache Spark Structured Streaming

Apache Kafka

MongoDB

Bloom Filter 

--------------------------------------------------------
 Main Features

✔️ Reads messages continuously from Kafka topic
✔️ Parses JSON fields (title, link, content, source, date)
✔️ Uses a Bloom Filter to ignore duplicates in memory
✔️ Cleans text from unwanted characters
✔️ Stores clean data into MongoDB database → collection news

--------------------------------------------------------
📂 Project Structure
src/
 ├─ consumer/SparkKafkaConsumer.scala
 └─ utils/BloomFilter.scala
 
 --------------------------------------------------------
 Kafka Input Format

Each incoming Kafka message should look like JSON:

{
  "title": "...",
  "link": "...",
  "content": "...",
  "date": "2024-01-10T12:30:00",
  "source": "BBC"
}

--------------------------------------------------------
 MongoDB Output

Stored as fields:

title

content

source

date

link

timestamp (Kafka ingestion timestamp)

-------------------------------------------------------
Database name is configured as:

dashboard_news.news

-------------------------------------------------------
Config inside Spark:

.config("spark.mongodb.output.uri", "mongodb://127.0.0.1/")
.config("spark.mongodb.output.database", "dashboard_news")
.config("spark.mongodb.output.collection", "news")

--------------------------------------------------------
 Kafka Configuration

Consumer subscribes to:

Topic: spark-news-stream-v2
Bootstrap: localhost:9092
Offsets: latest

-----------------------------------------------------
 Duplicate Detection (Bloom Filter)

The project uses a Bloom Filter from the Twitter Algebird library to prevent duplicate news from being stored in MongoDB. This library provides a built-in probabilistic set that automatically handles hashing and memory management, without needing manual bit arrays or snapshots.

Uniqueness is checked using a combined key of title + source, If the item already exists in the Bloom Filter, it is ignored; otherwise, it is accepted and stored.

--------------------------------------------------------

 Streaming Trigger

Spark writes to Mongo every: 3 seconds






