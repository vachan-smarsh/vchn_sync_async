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
