from typing import Dict

import json

from confluent_kafka import Producer

TOPIC: str = 'my-topic'
HOST: str = 'localhost'
PORT: int = 9092

producer_conf: Dict[str, str] = {
    'bootstrap.servers': f'{HOST}:{PORT}',
    'client.id': 'my-producer'
}
producer: Producer = Producer(producer_conf)
print('Producer:', producer)

data: Dict[str, str] = {
    'name': 'John',
    'age': 30,
    'city': 'New York',
}

json_data: str = json.dumps(data).encode('utf-8')

producer.produce(TOPIC, value=json_data)

producer.flush()
print('Sent!')
