from confluent_kafka import Producer
import time
import random

def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result."""
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def produce_data(topic):
    # Create Producer instance
    producer = Producer({'bootstrap.servers': 'localhost:9092'})

    try:
        for i in range(50):  # Produce 50 messages
            value = f'Message {i+1}'  # Message content
            # Produce message
            producer.produce(topic, value=value.encode('utf-8'), callback=delivery_report)
            # Sleep for a random amount of time between 0.5 and 2 seconds
            time.sleep(random.uniform(0.5, 2))
        print("Produced 50 messages successfully!")
    except KeyboardInterrupt:
        print("Producer interrupted, stopping...")
    finally:
        # Flush messages and wait for delivery reports
        producer.flush()
        # Close Producer
        

if __name__ == '__main__':
    topic = 'source-topic'  # Change this to your desired topic
    produce_data(topic)