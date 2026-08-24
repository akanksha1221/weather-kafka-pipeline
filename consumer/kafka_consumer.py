import os
import sys
import json
from json import loads

from kafka import KafkaConsumer
from s3fs import S3FileSystem

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import KAFKA_BROKER, KAFKA_TOPIC, S3_BUCKET, S3_PREFIX

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

s3 = S3FileSystem()

if __name__ == "__main__":
    for count, i in enumerate(consumer):
        path = "s3://{}/{}/weather_{}.json".format(S3_BUCKET, S3_PREFIX, count)
        with s3.open(path, 'w') as file:
            json.dump(i.value, file)
        print("Saved: {}".format(path))
