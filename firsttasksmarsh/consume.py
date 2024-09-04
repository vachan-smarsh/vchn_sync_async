import time
from confluent_kafka import Consumer, KafkaError
import psutil
import os

TOPIC_NAME = "test_topic"
BROKER = "localhost:9092"
BATCH_SIZE = 10000


def calculate(messages):
    words = ' '.join(messages).split()
    return len(words), len(set(words))


def consume_kafka_messages(consumer):
    total_consumed = 0
    batch_messages = []
    start = time.time()

    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(msg.error())
                break
        else:
            batch_messages.append(msg.value().decode('utf-8'))
            total_consumed += 1

            if total_consumed % BATCH_SIZE == 0:

                end=time.time()
                word_count, unique_word_count = calculate(batch_messages)
                total=end-start
                throughput= BATCH_SIZE / total if total > 0 else 0
                print(f"Batch Processed: {BATCH_SIZE} messages, {word_count} words, {unique_word_count} unique words")
                print(f"Overall throughput: {throughput:.2f} messages/sec")
                process = psutil.Process(os.getpid())
                print(f"CPU usage: {process.cpu_percent(interval=1.0):.2f}%")
                print(f"Memory usage: {process.memory_info().rss / 1024 / 1024:.2f} MB")

                batch_messages = []


def main():
    consumer = Consumer({
        'bootstrap.servers': BROKER,
        'group.id': 'my_group',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe([TOPIC_NAME])

    consume_kafka_messages(consumer)

    consumer.close()


if __name__ == '__main__':
    main()
