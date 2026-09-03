import os
import sys
import time

import boto3

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import S3_BUCKET, S3_PREFIX, AWS_REGION
from monitoring.push_metrics import push_s3_file_count, push_consumer_throughput

# Standalone poller — does NOT touch kafka_consumer.py. Every 60 seconds it
# counts the JSON files kafka_consumer.py has written to S3 and pushes that
# count, plus the delta since the last poll, to CloudWatch as custom metrics.

POLL_INTERVAL_SECONDS = 60

s3 = boto3.client('s3', region_name=AWS_REGION)


def count_s3_files():
    paginator = s3.get_paginator('list_objects_v2')
    count = 0
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX + "/"):
        count += page.get('KeyCount', 0)
    return count


if __name__ == "__main__":
    last_count = count_s3_files()
    print("Starting count: {}".format(last_count))

    while True:
        time.sleep(POLL_INTERVAL_SECONDS)

        current_count = count_s3_files()
        new_files = current_count - last_count
        messages_per_min = max(new_files, 0)

        push_s3_file_count(current_count)
        push_consumer_throughput(messages_per_min)
        print("S3FileCount={} ConsumerThroughput={}/min".format(current_count, messages_per_min))

        last_count = current_count
