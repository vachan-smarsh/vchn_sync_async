import csv
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

TOPIC_NAME = "test_topic"
BROKER = "localhost:9092"
BATCH_SIZE = 10000
TOTAL_RECORDS = 1000000

def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}]")

def create_topic(admin_client, topic_name):
    topic_list = admin_client.list_topics(timeout=10).topics
    if topic_name not in topic_list:
        new_topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
        admin_client.create_topics([new_topic])
        print(f"Topic '{topic_name}' created.")
    else:
        print(f"Topic '{topic_name}' already exists.")

def replicate(file_path, target_rows):
    with open(file_path, 'r') as csvfile:
        reader = list(csv.DictReader(csvfile))
        current_rows = len(reader)
        replication_factor = (target_rows // current_rows) + 1
        replicated_data = reader * replication_factor
        return replicated_data[:target_rows]

def produce_kafka_messages(producer, replicated_data):
    total_batches = len(replicated_data) // BATCH_SIZE

    for i in range(total_batches):
        batch = replicated_data[i * BATCH_SIZE:(i + 1) * BATCH_SIZE]
        for row in batch:
            producer.produce(TOPIC_NAME, value=str(row), callback=delivery_report)
        producer.poll(1)

        words = ' '.join(str(row) for row in batch).split()
        print(f"Batch {i+1}/{total_batches}: {len(batch)} messages, {len(words)} words")

    producer.flush()

def main():
    admin_client = AdminClient({'bootstrap.servers': BROKER})
    create_topic(admin_client, TOPIC_NAME)

    producer = Producer({'bootstrap.servers': BROKER})
    replicated_data = replicate('sample.csv', TOTAL_RECORDS)
    produce_kafka_messages(producer, replicated_data)

if __name__ == '__main__':
    main()

#docker stop $(docker ps -q)
#docker rm $(docker ps -a -q)