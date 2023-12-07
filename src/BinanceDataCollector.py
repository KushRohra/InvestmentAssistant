import requests, json
import os
from confluent_kafka import Producer
import json
import re
from datetime import datetime, timezone
import time

api_url = 'https://api.binance.com/api/v3/ticker/24hr'
time_url = 'https://api.binance.com/api/v3/time'

api_response = requests.get(api_url)
time_response = requests.get(time_url)

sys_time = time_response.json()

path = os.getcwd()

def format_data_to_send(data, filename):
    """
    should only send the following data fields : Timestamp, Open, High, Low, Close, Volume_(SUBJECT), Volume_(Currency), Weighted_Price
    """
    data['timestamp'] = extract_timestamp_from_file(filename)
    return str(data)

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def extract_timestamp_from_file(file_path):
    pattern = r'(\d{13})'
    match = re.findall(pattern, file_path)[0]
    timestamp_in_seconds = int(match)/1000.0
    dt = datetime.utcfromtimestamp(timestamp_in_seconds)
    dt = dt.replace(tzinfo=timezone.utc)

    # Format the datetime as a string
    formatted_date = dt.strftime('%Y-%m-%d %H:%M:%S%z')
    return formatted_date

if api_response.status_code == 200:
    # Parse the JSON data
    data = api_response.json()
    print(type(data))

    data = data[:500]

    # Specify the file path where you want to save the JSON data
    file_path = "data/{time}_binance_crypto_output.json".format(time=sys_time['serverTime'])
    # file_path = "{path}/data/{time}_binance_data.json".format(path=path, time=sys_time)

    kafka_topic = 'test'
    conf = {'bootstrap.servers': 'localhost:9092'}
    producer = Producer(conf)

    for record in data:
        # formatted_data = format_data_to_send(record, file_path)
        print(f"record: {record}")
        # print(f"formatted_data: {formatted_data}")
        producer.produce(kafka_topic, key='key', value=json.dumps(record), callback=delivery_report)
        # time.sleep(10)

    # Write the JSON data to the file
    with open(file_path, 'w') as json_file:
        json.dump(data, json_file, indent=2)

    print(f"JSON data has been saved to {file_path}")
else:
    print(f"Failed to retrieve data. Status code: {api_response.status_code}")
