# Kafka Producer and Consumer

## Overview

This repository contains a Kafka producer and consumer implementation using Python, Docker, and Docker Compose. The producer reads data from a CSV file, replicates it to reach a target number of records, and sends the data to a Kafka topic. The consumer reads messages from the Kafka topic, calculates metrics such as word count, unique word count, and throughput, and logs CPU and memory usage.

## Repository Structure

- `pro.py`: The Kafka producer script.
- `cond.py`: The Kafka consumer script.
- `Dockerfile`: Dockerfile to build the Python application image.
- `docker-compose.yaml`: Docker Compose file to set up Kafka, Zookeeper, and the Python application.
- `requirements.txt`: Python dependencies for the application.
- `README`: Information about the repository

## Project Setup and Configuration

### Environment Setup

1. **Apache Kafka and Zookeeper**:
   - Kafka and Zookeeper are set up using Docker to provide a consistent environment.
   - Docker Compose is used to manage the Kafka and Zookeeper containers.

2. **Python and Dependencies**:
   - Python scripts are used for Kafka producer and consumer.
   - The `confluent-kafka` library is used for Kafka integration in Python.

3. **Docker Setup**:
   - Docker is used to create isolated environments for running Kafka and Zookeeper.
   - Docker Compose is configured to manage Kafka and Zookeeper services.

### Kafka Setup

1. **Kafka Configuration**:
   - The Kafka broker is set up with default configurations for development and testing.
   - Topics are created as needed for message production and consumption.

2. **Topic Creation**:
   - Topics are created manually or through code to handle message production and consumption.
## How to Start the Producer and Consumer

1. **Set Up Docker and Docker Compose**:
   Ensure you have Docker and Docker Compose installed on your system. If not, install them following the instructions on the [Docker website](https://docs.docker.com/get-docker/) and the [Docker Compose documentation](https://docs.docker.com/compose/install/).

2. **Build and Start Services**:
   Use the following command to build the Docker images and start the services:
   ```bash
   docker compose up --build 
   python3 pro.py 
   python3 cond.py

## How It Works

### Producer (pro.py)

1. **Topic Creation**:
  The producer script first checks if the Kafka topic exists and creates it if it doesn't. 
2. **Data Replication**:
   It reads a CSV file, replicates the data to meet the target record count, and sends the data to the Kafka topic in batches.
3. **Async Production**:
    Messages are produced asynchronously using asyncio to handle batch processing efficiently.

### Consumer (cond.py)

1. **Message Consumption** :
   The consumer script continuously polls for messages from the Kafka topic, processes them in batches, and calculates metrics like word count, unique word count, and throughput.
2. **Performance Metrics** :
   It logs the CPU usage, memory usage, and throughput for each batch of messages consumed.

## Performance Measurement

1. **Add Performance Measurement Code** :
  Add code to measure the total time taken to produce all 1 million messages
  Example:

````
import time

start_time = time.time()
await produce_msg(producer, replicated_data)
end_time = time.time()
print(f"Time taken to produce all messages: {end_time - start_time:.2f} seconds")

````
2.**Compare Performance**:
Run the producer with the older method (without asyncio and batching).
Run the producer with the current implementation (using asyncio and batching).
Measure and compare the time taken to produce all messages in both cases.

## Intital code

`pro.py`
The initial producer code read a CSV file, replicated the data to reach a target number of records, and sent messages to Kafka in batches. The code used asynchronous methods to handle Kafka message production.

````
import csv
import asyncio
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

TOPIC_NAME = "test_topic"
BROKER = "localhost:9092"
BATCH_SIZE = 10000
TOTAL_RECORDS = 1000000

async def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}]")

async def create_topic(admin_client, topic_name):
    topic_list = admin_client.list_topics(timeout=10).topics
    if topic_name not in topic_list:
        new_topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
        admin_client.create_topics([new_topic])
        print(f"Topic '{topic_name}' created.")
    else:
        print(f"Topic '{topic_name}' already exists.")

async def replicate_csv(file_path, target_rows):
    async with aiofiles.open(file_path, mode='r') as csvfile:
        reader = csv.DictReader(await csvfile.read())
        reader = list(reader)
        current_rows = len(reader)
        replication_factor = (target_rows // current_rows) + 1
        replicated_data = reader * replication_factor
        return replicated_data[:target_rows]

async def produce_kafka_messages(producer, replicated_data):
    total_batches = len(replicated_data) // BATCH_SIZE

    for i in range(total_batches):
        batch = replicated_data[i * BATCH_SIZE:(i + 1) * BATCH_SIZE]
        for row in batch:
            producer.produce(TOPIC_NAME, value=str(row), callback=await delivery_report)
            await asyncio.sleep(0)

        await producer.flush()
        words = ' '.join(str(row) for row in batch).split()
        print(f"Batch {i+1}/{total_batches}: {len(batch)} messages, {len(words)} words")

async def main():
    admin_client = AdminClient({'bootstrap.servers': BROKER})
    await create_topic(admin_client, TOPIC_NAME)

    producer = Producer({'bootstrap.servers': BROKER})
    replicated_data = await replicate_csv('sample.csv', TOTAL_RECORDS)
    await produce_kafka_messages(producer, replicated_data)

if __name__ == '__main__':
    asyncio.run(main())
````

`cond.py`
The initial consumer code consumed messages from Kafka and provided batch-wise throughput metrics. It consumed a fixed number of messages and displayed resource usage statistics.
````
import asyncio
import time
from confluent_kafka import Consumer, KafkaError
import psutil
import os

TOPIC_NAME = "test_topic"
BROKER = "localhost:9092"
TOTAL_MESSAGES_TO_CONSUME = 1000
BATCH_SIZE = 10000

async def calculate(messages):
    words = ' '.join(messages).split()
    return len(words), len(set(words))

async def consume_kafka_messages(consumer, total_messages_to_consume):
    messages = []
    start_time = time.time()
    total_consumed = 0

    while total_consumed < total_messages_to_consume:
        msg = await consumer.poll(timeout=1.0)
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

        if total_consumed % BATCH_SIZE == 0:
            word_count, unique_word_count = await calculate(messages)
            print(f"Batch consumed: {len(messages)} messages, {word_count} words, {unique_word_count} unique words")
            messages = []

    end_time = time.time()
    total_time = end_time - start_time

    overall_throughput = total_consumed / total_time if total_time > 0 else 0
    per_message_throughput = overall_throughput / total_messages_to_consume if total_messages_to_consume > 0 else 0

    print(f"Total messages consumed: {total_consumed}")
    print(f"Overall throughput: {overall_throughput:.2f} messages/second")
    print(f"Per-message processing throughput: {per_message_throughput:.2f} messages/second")

    process = psutil.Process(os.getpid())
    print(f"CPU usage: {process.cpu_percent(interval=1.0):.2f}%")
    print(f"Memory usage: {process.memory_info().rss / 1024 / 1024:.2f} MB")

async def main():
    consumer = Consumer({
        'bootstrap.servers': BROKER,
        'group.id': 'my_group',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe([TOPIC_NAME])

    await consume_kafka_messages(consumer, TOTAL_MESSAGES_TO_CONSUME)

    consumer.close()

if __name__ == '__main__':
    asyncio.run(main())
````
### Throughput Output
Producer Throughput: Initial tests showed throughput varying significantly based on the batch size and message production rate.

Consumer Throughput: Initially, the consumer processed a fixed number of messages and displayed throughput in messages per second.

## Final Code

`pro.py`

````
import csv
import asyncio
import aiofiles
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

MAX_TASKS = 250
TOPIC_NAME = "test_topic"
BROKER = "localhost:9092"
BATCH_SIZE = 10000
TOTAL_RECORDS = 1000000

def delivery_report(err, msg):
    if err:
        print(f"Delivery did not occur: {err}")
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}]")

async def create_topic(admin_client, topic_name):
    topic_list = admin_client.list_topics(timeout=10).topics
    if topic_name not in topic_list:
        new_topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
        admin_client.create_topics([new_topic])
        print(f"Topic '{topic_name}' created.")
    else:
        print(f"Topic '{topic_name}' already present.")

async def replicate(file_path, target_rows):
    async with aiofiles.open(file_path, 'r') as csvfile:
        reader = list(csv.DictReader(await csvfile.read()))
        cur_rows = len(reader)
        rfactor = (target_rows // cur_rows) + 1
        replicated_data = reader * rfactor
        return replicated_data[:target_rows]

async def produce(producer, message):
    producer.produce(TOPIC_NAME, value=message, callback=delivery_report)

async def produce_msg(producer, replicated_data):
    total_batches = len(replicated_data) // BATCH_SIZE

    for i in range(total_batches):
        batch = replicated_data[i * BATCH_SIZE:(i + 1) * BATCH_SIZE]
        tasks = []
        for row in batch:
            t = asyncio.create_task(produce(producer, str(row)))
            tasks.append(t)
            if len(tasks) >= MAX_TASKS:
                await asyncio.gather(*tasks)
                tasks = []

        if tasks:
            await asyncio.gather(*tasks)

        producer.poll(1)

        words = ' '.join(str(row) for row in batch).split()
        print(f"Batch {i+1}/{total_batches}: {len(batch)} messages, {len(words)} words")

    producer.flush()

async def main():
    admin_client = AdminClient({'bootstrap.servers': BROKER})
    await create_topic(admin_client, TOPIC_NAME)
    producer = Producer({'bootstrap.servers': BROKER})
    replicated_data = await replicate('sample.csv', TOTAL_RECORDS)
    await produce_msg(producer, replicated_data)

if __name__ == '__main__':
    asyncio.run(main())


````
`cond.py`

````
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
            start_time = time.time()

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

````
### Throughput of Synchronous Code

````
Batch Processed: 10000 messages, 249069 words, 11743 unique words
Overall throughput: 2375.29 messages/sec
CPU usage: 2.00%
Memory usage: 92.38 MB
Batch Processed: 10000 messages, 250971 words, 11259 unique words
Overall throughput: 1905.54 messages/sec
CPU usage: 0.00%
Memory usage: 94.33 MB
Batch Processed: 10000 messages, 253070 words, 10837 unique words
Overall throughput: 1592.69 messages/sec
CPU usage: 0.00%
Memory usage: 98.12 MB
````
### Throughput of Asynchronous Code

````
Batch consumed: 10000 messages
Total number of words: 73264
Number of unique words: 2778
Overall throughput: 25654176.91 messages/second
Total time to consume this batch : 0.01949000358581543 seconds
CPU usage: 1.00%
Memory usage: 71.12 MB

Batch consumed: 10000 messages
Total number of words: 68329
Number of unique words: 1985
Overall throughput: 24762916.78 messages/second
Total time to consume this batch : 0.020595312118530273 seconds
CPU usage: 0.00%
Memory usage: 71.74 MB

````

## Steps to get the output

1. **Set Up Kafka Environment**:
   - Configured Kafka and Zookeeper using Docker Compose to ensure a consistent development environment.
   - Created Docker Compose configurations to define and run Kafka and Zookeeper containers.

2. **Configured Python Environment**:
   - Installed the `confluent-kafka` library to interact with Kafka from Python.
   - Set up Python scripts for Kafka producer and consumer.

3. **Implemented Asynchronous Producer**:
   - Developed a Kafka producer that sends messages asynchronously.
   - Configured the producer to use `asyncio` for handling asynchronous message production.
   - Used `asyncio.sleep` to control the rate of message production.

4. **Implemented Asynchronous Consumer**:
   - Developed a Kafka consumer that asynchronously consumes messages.
   - Configured the consumer to use `asyncio` for handling asynchronous message consumption.
   - Implemented error handling to manage and report Kafka errors.

5. **Updated Docker and Kafka Configuration**:
   - Ensured Kafka and Zookeeper are running in Docker containers, with proper configurations for development.
   - Updated Docker Compose files to reflect changes in Kafka and Zookeeper settings.

6. **Testing and Validation**:
   - Tested the Kafka producer and consumer to ensure they work correctly in an asynchronous environment.
   - Verified that the producer sends messages and the consumer receives and processes them as expected.

7. **Code Improvements**:
   - Refined the asynchronous handling of messages to optimize performance and reliability.
   - Adjusted sleep intervals and error handling in the producer and consumer scripts.

8. **Documentation**:
   - Created a `README.md` file to document the setup, configuration, and usage of the Kafka producer and consumer.
   - Provided instructions on how to set up and run the Kafka environment and Python scripts.

9. **Repository Maintenance**:
   - Added the `README.md` file to the repository to provide an overview and instructions for future users.
   - Cleaned up the repository by removing unnecessary files and ensuring that the project structure is organized.

## Changes and Improvements

### 1. **Initial Code: Basic Producer and Consumer**

**Initial Producer Code:**

```python
from confluent_kafka import Producer

producer = Producer({'bootstrap.servers': 'localhost:9092'})
producer.produce('test_topic', key='key', value='value')
producer.flush()
```
**Initial Consumer code:**

````python
from confluent_kafka import Consumer, KafkaError

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my_group',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe(['test_topic'])

msg = consumer.poll(timeout=1.0)
if msg is None:
    print("No message received")
elif msg.error():
    print(f"Consumer error: {msg.error()}")
else:
    print(f"Received message: {msg.value().decode('utf-8')}")
consumer.close()
````

### Explanation:

The initial code sets up basic Kafka producer and consumer functionalities. However, it does not handle large-scale data efficiently or provide detailed performance metrics.

**2. Adding Asynchronous Capabilities**

Improved Producer Code:

````python
import asyncio
from confluent_kafka import Producer

async def produce_message(producer, message):
    loop = asyncio.get_running_loop()
    producer.produce('test_topic', value=message, callback=lambda err, msg: loop.call_soon_threadsafe(asyncio.create_task, delivery_report(err, msg)))
    producer.poll(1)

````
### Explanation:

Introduced asynchronous message production to handle higher throughput and improve performance.
asyncio.create_task is used for non-blocking message delivery.
Improved Consumer Code:
````python
import asyncio
from confluent_kafka import Consumer, KafkaError

async def consume_messages(consumer):
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
            print(f"Received message: {msg.value().decode('utf-8')}")

````
### Explanation:

Implemented asynchronous message consumption to improve efficiency and responsiveness.
Uses await asyncio.sleep(0) to avoid blocking operations.

**3. Adding Batching and Replication**
Batching in Producer Code:
````python
async def produce_batch(producer, data):
    for batch in chunked(data, BATCH_SIZE):
        tasks = [asyncio.create_task(produce_message(producer, msg)) for msg in batch]
        await asyncio.gather(*tasks)
        print(f"Produced batch of {len(batch)} messages")
````

### Explanation:

Added batching to the producer to handle large volumes of data more effectively.
chunked(data, BATCH_SIZE) splits data into manageable batches.
Batching in Consumer Code:

````python
async def consume_batch(consumer):
    messages = []
    while True:
        msg = consumer.poll(timeout=0.1)
        if msg:
            messages.append(msg.value().decode('utf-8'))
            if len(messages) >= BATCH_SIZE:
                process_batch(messages)
                messages.clear()
````

### Explanation:
Implemented batching in the consumer to process messages in groups.
process_batch(messages) processes each batch for performance metrics.

**4. Final Code with Advanced Features**

Final Producer Code (pro.py):
````python
import csv
import asyncio
import aiofiles
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

MAX_TASKS = 250
TOPIC_NAME = "test_topic"
BROKER = "localhost:9092"
BATCH_SIZE = 10000
TOTAL_RECORDS = 1000000

def delivery_report(err, msg):
    if err:
        print(f"Delivery did not occur: {err}")
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}]")

async def create_topic(admin_client, topic_name):
    topic_list = admin_client.list_topics(timeout=10).topics
    if topic_name not in topic_list:
        new_topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
        admin_client.create_topics([new_topic])
        print(f"Topic '{topic_name}' created.")
    else:
        print(f"Topic '{topic_name}' already present.")

async def replicate(file_path, target_rows):
    async with aiofiles.open(file_path, 'r') as csvfile:
        reader = list(csv.DictReader(await csvfile.read()))
        cur_rows = len(reader)
        rfactor = (target_rows // cur_rows) + 1
        replicated_data = reader * rfactor
        return replicated_data[:target_rows]

async def produce(producer, message):
    producer.produce(TOPIC_NAME, value=message, callback=delivery_report)

async def produce_msg(producer, replicated_data):
    total_batches = len(replicated_data) // BATCH_SIZE

    for i in range(total_batches):
        batch = replicated_data[i * BATCH_SIZE:(i + 1) * BATCH_SIZE]
        tasks = []
        for row in batch:
            t = asyncio.create_task(produce(producer, str(row)))
            tasks.append(t)
            if len(tasks) >= MAX_TASKS:
                await asyncio.gather(*tasks)
                tasks = []

        if tasks:
            await asyncio.gather(*tasks)

        producer.poll(1)

        words = ' '.join(str(row) for row in batch).split()
        print(f"Batch {i+1}/{total_batches}: {len(batch)} messages, {len(words)} words")

    producer.flush()

async def main():
    admin_client = AdminClient({'bootstrap.servers': BROKER})
    await create_topic(admin_client, TOPIC_NAME)
    producer = Producer({'bootstrap.servers': BROKER})
    replicated_data = await replicate('sample.csv', TOTAL_RECORDS)
    await produce_msg(producer, replicated_data)

if __name__ == '__main__':
    asyncio.run(main())

````

Final Consumer Code (cond.py):

````python
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
````
### Explanation:

Asynchronous Handling: Implemented asyncio for handling message production and consumption asynchronously, which allows for non-blocking operations and improved throughput.
Batching: Introduced batching to handle large volumes of messages more effectively and ensure efficient processing.
Replication: Added functionality to replicate data to meet the target record count.
Metrics: Added performance metrics for tracking throughput, CPU usage, and memory usage.
How to Run the Code
Setup Kafka:

Ensure Kafka and Zookeeper are running.

Run the Producer:

Execute the producer script:

````
python3 pro.py
````

Run the Consumer:

Execute the consumer script:

````bash
python3 cond.py
````