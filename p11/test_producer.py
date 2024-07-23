from confluent_kafka import Producer

def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result."""
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def produce_data():
    # Create Producer instance
    producer = Producer({'bootstrap.servers': 'localhost:9092'})

    # Produce Data
    producer.produce('my-topic', value='Hello, Kafka!', callback=delivery_report)

    # Flush messages
    producer.flush()

if __name__ == '__main__':
    produce_data()