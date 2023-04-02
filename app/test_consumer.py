from typing import Dict, Optional

import json

from confluent_kafka import Consumer

TOPIC: str = 'my-topic'
HOST: str = 'localhost'
PORT: int = 9092

consumer_conf: Dict[str, str] = {
    'bootstrap.servers': f'{HOST}:{PORT}',
    'group.id': 'my-group',
    'auto.offset.reset': 'earliest'
}
consumer: Consumer = Consumer(consumer_conf)
print('Consumer:', consumer)

consumer.subscribe([TOPIC])

while True:
    msg: Optional[object] = consumer.poll(1.0)

    # Handle any errors that occur while polling
    if msg is None:
        print('Polling')
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            print('Reached end of partition')
        else:
            print('Error while polling: {}'.format(msg.error()))
        continue

    # Process the message
    print('Received message: {}'.format(msg.value().decode('utf-8')))
    break

# Close the consumer connection
consumer.close()
