import os
import sys
import json
from json import loads

import boto3
from kafka import KafkaConsumer

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import KAFKA_BROKER, KAFKA_TOPIC, S3_BUCKET, S3_PREFIX, AWS_REGION, SNS_TOPIC_ARN

# Runs alongside kafka_consumer.py (separate consumer group, same topic).
# kafka_consumer.py handles the S3 write; this script only fans the
# same records out to SNS so alert/transform Lambdas can react to them.
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    group_id="weather_sns_publisher",
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

sns = boto3.client('sns', region_name=AWS_REGION)

if __name__ == "__main__":
    for count, i in enumerate(consumer):
        message = {
            'bucket': S3_BUCKET,
            'key': "{}/weather_{}.json".format(S3_PREFIX, count),
            'record': i.value
        }
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=json.dumps(message),
            Subject='New weather record'
        )
        print("Published to SNS: {}".format(message['key']))
