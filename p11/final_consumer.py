import time
from confluent_kafka import Consumer, KafkaError
import pymongo

def consume_and_insert(processed_topic):
    # Connect to MongoDB
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    # Create or access the database
    mydb = client["mydatabase"]
    # Create or access the collection
    mycol = mydb["processed_data"]

    # Create Kafka consumer configuration
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'mongo_consumer_group',
        'auto.offset.reset': 'earliest'
    }

    # Create Kafka consumer
    consumer = Consumer(conf)

    # Subscribe to the processed topic
    consumer.subscribe([processed_topic])

    try:
        while True:
            # Poll for messages
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition, no more messages
                    continue
                else:
                    # Other error
                    print(msg.error())
                    break

            # Message consumed successfully, insert into MongoDB
            message = msg.value().decode('utf-8')
            mydict = {"data": message}
            mycol.insert_one(mydict)

            print("Data inserted into MongoDB:", message)

    except KeyboardInterrupt:
        pass
    finally:
        # Close the Kafka consumer
        consumer.close()

if __name__ == '__main__':
    processed_topic = 'proces-topic'
    consume_and_insert(processed_topic)