import time
import random
from confluent_kafka import Consumer, Producer

def remove_vowels(data):
    vowels = 'aeiouAEIOU'
    processed_data = ''.join(char for char in data if char not in vowels)
    return processed_data

def process_message(message):
    processed_data = remove_vowels(message)
    return processed_data

def consume_and_process(source_topic, processed_topic, max_messages=100, timeout_seconds=60):
    consumer_conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'process_group',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([source_topic])

    producer_conf = {'bootstrap.servers': 'localhost:9092'}
    producer = Producer(producer_conf)

    messages_processed = 0
    last_message_time = time.time()

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("Consumer error: {}".format(msg.error()))
                continue

            message = msg.value().decode('utf-8')
            processed_message = process_message(message)
            
            producer.produce(processed_topic, value=processed_message.encode('utf-8'))
            print("Message delivered to {} [{}]".format(processed_topic, msg.partition()))

            messages_processed += 1
            last_message_time = time.time()  # Update last message time

            if messages_processed >= max_messages or time.time() - last_message_time > timeout_seconds:
                print("Processed {} messages or reached timeout. Exiting loop.".format(messages_processed))
                break

    except KeyboardInterrupt:
        print("Interrupted. Closing consumer and producer.")
        consumer.close()
        producer.flush()
        producer.close()

if __name__ == '__main__':
    source_topic = 'source-topic'
    processed_topic = 'proces-topic'
    consume_and_process(source_topic, processed_topic)