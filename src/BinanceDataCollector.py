import requests, json
import os
import time

from confluent_kafka import Producer
from KafkaProducer import push_to_kafka

api_url = 'https://api.binance.com/api/v3/ticker/24hr'
time_url = 'https://api.binance.com/api/v3/time'

api_response = requests.get(api_url)
time_response = requests.get(time_url)

sys_time = time_response.json()

path = os.getcwd()

kafka_topic = 'test'
conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)

if api_response.status_code == 200:
    # Parse the JSON data
    data = api_response.json()

    # Specify the file path where you want to save the JSON data
    file_path = "{path}/data/{time}_binance_data.json".format(path=path, time=sys_time)

    # Write the JSON data to the file
    with open(file_path, 'w') as json_file:
        json.dump(data, json_file)

    for record in data:
        if record['symbol'].endswith("USDT"):
            print(record)
            push_to_kafka(producer, kafka_topic, json.dumps(record))
            time.sleep(0.05)

    print(f"JSON data has been saved to {file_path}")
else:
    print(f"Failed to retrieve data. Status code: {api_response.status_code}")
