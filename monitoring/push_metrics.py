import os
import sys

import boto3

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import CLOUDWATCH_NAMESPACE, AWS_REGION

cloudwatch = boto3.client('cloudwatch', region_name=AWS_REGION)


def push_s3_file_count(count):
    cloudwatch.put_metric_data(
        Namespace=CLOUDWATCH_NAMESPACE,
        MetricData=[{
            'MetricName': 'S3FileCount',
            'Value': count,
            'Unit': 'Count'
        }]
    )


def push_consumer_throughput(messages_per_min):
    cloudwatch.put_metric_data(
        Namespace=CLOUDWATCH_NAMESPACE,
        MetricData=[{
            'MetricName': 'ConsumerThroughput',
            'Value': messages_per_min,
            'Unit': 'Count/Second'
        }]
    )
