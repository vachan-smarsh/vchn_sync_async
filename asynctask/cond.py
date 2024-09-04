import asyncio
import time
from confluent_kafka import Consumer, KafkaError
import psutil
import os

TOPIC_NAME = "test_topic"
BROKER = "localhost:9092"
BATCH_SIZE = 10000

async def calculate(messages):
    words = ' '.join(messages).split()
    return len(words), len(set(words))

async def consume_msg(consumer):
    messages = []
    total_consumed = 0
    st = time.time()

    while True:
        msg = consumer.poll(timeout=0.1)
        if msg is None:
            await asyncio.sleep(0)
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(msg.error())
                break
        else:
            messages.append(msg.value().decode('utf-8'))
            total_consumed += 1

        if len(messages) >= BATCH_SIZE:
            end = time.time()
            total_time = end - st

            word_count, unique_word_count = await calculate(messages)
            overall_throughput = total_consumed / total_time if total_time > 0 else 0

            print(f"Batch consumed: {len(messages)} messages")
            print(f"Total number of words: {word_count}")
            print(f"Number of unique words found: {unique_word_count}")
            print(f"Throughput of this batch: {overall_throughput:.2f} messages/second")
            print(f"Total time to consume this batch : {total_time} seconds")
            process = psutil.Process(os.getpid())
            print(f"CPU usage: {process.cpu_percent(interval=1.0):.2f}%")
            print(f"Memory usage: {process.memory_info().rss / 1024 / 1024:.2f} MB")

            messages.clear()
            st = time.time()

    consumer.close()

async def main():
    consumer = Consumer({
        'bootstrap.servers': BROKER,
        'group.id': 'my_group',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe([TOPIC_NAME])

    await consume_msg(consumer)

if __name__ == '__main__':
    asyncio.run(main())
