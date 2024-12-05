from confluent_kafka import Producer
import json
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
import csv
from datetime import datetime

KAFKA_SERVERS = "your-broker1.kafka.us-east-1.amazonaws.com:9096,your-broker2.kafka.us-east-1.amazonaws.com:9096,your-broker3.kafka.us-east-1.amazonaws.com:9096"
TOPIC_NAME = "topic-name"

config = {
    'bootstrap.servers': KAFKA_SERVERS,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'SCRAM-SHA-512',
    'sasl.username': 'kafka_platform_ck',
    'sasl.password': 'example123'
}

producer = Producer(config)
partition_count = 3 

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")
def generate_uuid():
    return str(uuid.uuid4())

def get_formatted_timestamp():
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]

def generate_message(row):
    document = row['document'].strip().zfill(11)
    
    payload = {
        "event": "topic_name",
        "document": document,
        "decisionId": 12345,
        "infoId": "info_id_value",
        "subInfoId": "sub_info_id_value",
        "xValue": 1.23,
        "yValue": 4.56,
        "zValue": 7.89,
        "floatNumber": 10.11,
        "string": "string_value",
        "correlationId": generate_uuid(),
        "timestamp": get_formatted_timestamp()
        }

    return json.dumps(payload)

def send_message(row, partition):
    payload = generate_message(row)
    producer.produce(TOPIC_NAME, value=payload, partition=partition, callback=delivery_report)
    producer.poll(0)

def send_messages_in_parallel(rows, thread_count):
    start_time = time.time()
    with ThreadPoolExecutor(max_workers=thread_count) as executor:
        for i, row in enumerate(rows):
            partition = i % partition_count  # Distribui as mensagens entre as partições 0, 1 e 2
            executor.submit(send_message, row, partition)
    producer.flush()
    elapsed_time = time.time() - start_time
    print(f"{len(rows)} mensagens enviadas em {elapsed_time:.2f} segundos")

def read_documents_from_csv(filename):
    rows = []
    with open(filename, mode='r') as file:
        csv_reader = csv.DictReader(file)
        for row in csv_reader:
            rows.append(row)
    return rows

if __name__ == '__main__':
    filename = 'data.csv'
    rows = read_documents_from_csv(filename)
    thread_count = 80  # Número de threads para enviar mensagens em paralelo
    send_messages_in_parallel(rows, thread_count)

