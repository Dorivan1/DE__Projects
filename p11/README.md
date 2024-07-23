#  Kafka with MongoDB Integration

This project demonstrates the use of Kafka to produce random messages, process those messages, and store them in MongoDB.

## Overview

In this project, we:
1. Set up a Kafka producer to generate and send messages to a Kafka topic.
2. Created a consumer to read these messages from the topic, process them by removing vowels, and send them to a new topic.
3. Set up a final consumer to read the processed messages and insert them into a MongoDB collection.

## Steps

1. **Start Kafka and Zookeeper**
   - Use the provided `zookeeper_kafka_commands.txt` file to start Zookeeper and Kafka servers.

2. **Run the Kafka Producer**
   - Use `test_producer.py` or `producer.py` to generate messages.
   - `test_producer.py` sends a single message for testing.
   - `producer.py` sends 50 random messages to a Kafka topic.

3. **Process Messages**
   - Use `consumer_process.py` to consume messages from the source topic, process them to remove vowels, and produce them to a new Kafka topic.

4. **Insert Processed Messages into MongoDB**
   - Use `final_consumer.py` to consume the processed messages from Kafka and insert them into a MongoDB collection.

5. **Verify Data in MongoDB**
   - Use `conn_mongo.py` to connect to MongoDB and print all documents in the `processed_data` collection.

## Conclusion

This project serves as a demonstration of how to set up Kafka, produce and consume messages, process them, and store the results in MongoDB.