import csv
import json
import glob
import os
import time

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

INT_FIELDS = [
    'id', 'customer_age', 'product_quantity',
    'sale_customer_id', 'sale_seller_id', 'sale_product_id',
    'sale_quantity', 'product_reviews',
]
FLOAT_FIELDS = [
    'product_price', 'sale_total_price',
    'product_weight', 'product_rating',
]


def wait_for_kafka(bootstrap_servers, max_retries=30, retry_interval=5):
    for i in range(max_retries):
        try:
            p = KafkaProducer(bootstrap_servers=bootstrap_servers)
            p.close()
            print("Connected to Kafka")
            return
        except NoBrokersAvailable:
            print(f"Waiting for Kafka... ({i + 1}/{max_retries})")
            time.sleep(retry_interval)
    raise RuntimeError("Could not connect to Kafka")


def convert_types(row):
    for field in INT_FIELDS:
        val = row.get(field, '')
        if val and val.strip():
            try:
                row[field] = int(val)
            except (ValueError, TypeError):
                row[field] = None
        else:
            row[field] = None

    for field in FLOAT_FIELDS:
        val = row.get(field, '')
        if val and val.strip():
            try:
                row[field] = float(val)
            except (ValueError, TypeError):
                row[field] = None
        else:
            row[field] = None

    for key, val in row.items():
        if isinstance(val, str) and not val.strip():
            row[key] = None

    return row


def main():
    bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    topic = os.environ.get('KAFKA_TOPIC', 'mock_data')
    data_dir = os.environ.get('DATA_DIR', '/data')

    wait_for_kafka(bootstrap_servers)

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
    )

    csv_files = sorted(glob.glob(os.path.join(data_dir, '*.csv')))
    print(f"Found {len(csv_files)} CSV files in {data_dir}")

    total_sent = 0
    for filepath in csv_files:
        filename = os.path.basename(filepath)
        print(f"Processing {filename}...")
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                record = convert_types(dict(row))
                producer.send(topic, value=record)
                total_sent += 1
                if total_sent % 1000 == 0:
                    producer.flush()
                    print(f"  Sent {total_sent} records...")
        print(f"  Finished {filename}")

    producer.flush()
    producer.close()
    print(f"Done! Total records sent: {total_sent}")


if __name__ == '__main__':
    main()
