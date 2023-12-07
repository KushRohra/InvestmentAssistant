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
        print(event.is_directory)
        if event.is_directory:
            return

        file_path = event.src_path
        file_timestamp = extract_timestamp_from_file(file_path)

        global latest_timestamp
        print(f"latest_timestamp: {latest_timestamp}")
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


def send_file_to_kafka(file_path):
    print(file_path)
    # with open(file_path, 'r') as file:
    #     json_string = file.read()   
    # print(type(json_string))
    # data = json.loads(json_string)
    # print(type(data))
    buffer_size = 8192  # Set your desired buffer size

    with open('your_large_file.txt', 'rb') as file:
        while True:
            chunk = file.read(buffer_size)
            if not chunk:
                break 
            for sym in chunk:
                print(sym)
                print(type(sym))
                formatted_data = format_data_to_send(sym, file_path)
                producer.produce(kafka_topic, key='key', value=formatted_data, callback=delivery_report)
    # with open(file_path, 'r') as data_file:
    #     crypto_binance_data = json.load(data_file)
    #     print(type(crypto_binance_data))
        # for sym in crypto_binance_data:
        #     print(sym)
        #     print(type(sym))
        #     formatted_data = format_data_to_send(sym, file_path)
        #     producer.produce(kafka_topic, key='key', value=formatted_data, callback=delivery_report)


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
