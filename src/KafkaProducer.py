import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from confluent_kafka import Producer
from datetime import datetime, timezone
import os
import json
import re


# Kafka producer configuration
kafka_topic = 'test'
conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)


# Directory to monitor
directory_to_monitor = "{}/data".format(os.getcwd())

# Keep track of the latest processed timestamp
latest_timestamp = None


class FileEventHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return

        file_path = event.src_path
        file_timestamp = extract_timestamp_from_file(file_path)

        global latest_timestamp
        if latest_timestamp is None or file_timestamp > latest_timestamp:
            # New file found
            latest_timestamp = file_timestamp
            send_file_to_kafka(file_path)


def extract_timestamp_from_file(file_path):
    pattern = r'(\d{13})'
    match = re.findall(pattern, file_path)[0]
    timestamp_in_seconds = int(match)/1000.0
    dt = datetime.utcfromtimestamp(timestamp_in_seconds)
    dt = dt.replace(tzinfo=timezone.utc)

    # Format the datetime as a string
    formatted_date = dt.strftime('%Y-%m-%d %H:%M:%S%z')
    return formatted_date


def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


def push_to_kafka(kafka_producer, topic, formatted_data):
    kafka_producer.produce(topic, value=formatted_data, callback=delivery_report)

def send_file_to_kafka(file_path):
    with open(file=file_path) as data_file:
        crypto_binance_data = json.loads(data_file.read())
        for sym in crypto_binance_data:
            formatted_data = format_data_to_send(sym, file_path)
            push_to_kafka(producer, kafka_topic, formatted_data)
            


def format_data_to_send(data, filename):
    """
    should only send the following data fields : Timestamp, Open, High, Low, Close, Volume_(SUBJECT), Volume_(Currency), Weighted_Price
    """
    data['timestamp'] = extract_timestamp_from_file(filename)
    return str(data)


if __name__ == "__main__":
    event_handler = FileEventHandler()
    observer = Observer()
    observer.schedule(event_handler, path=directory_to_monitor, recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()
