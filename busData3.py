from pykafka import KafkaClient
from datetime import datetime
import json
import uuid
import time
# our topic is: busData


client = KafkaClient(hosts="localhost:9092")
topic = client.topics['geo1']
producer = topic.get_sync_producer()


file = open('./data/bus3.json')
json_array = json.load(file)
coordinates = json_array['features'][0]['geometry']['coordinates']  # as List

data = {}
data['busline'] = '00003'


def generate_checkpoint(coordinates):
    i = 0
    while i < len(coordinates):
        data['key'] = data['busline'] + '_' + str(uuid.uuid4())
        data['timestamp'] = str(datetime.utcnow())
        data['lat'] = coordinates[i][0]
        data['long'] = coordinates[i][1]
        message = json.dumps(data)
        print(message)
        # send message to topic.
        producer.produce(message.encode('ascii'))
        time.sleep(1)
        if i == len(coordinates)-1:
            i = 0
        else:
            i += 1


generate_checkpoint(coordinates)
