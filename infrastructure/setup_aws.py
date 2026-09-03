import json

import boto3

AWS_REGION = "us-east-1"
ACCOUNT_ID = boto3.client('sts', region_name=AWS_REGION).get_caller_identity()['Account']

SNS_TOPIC_NAME = "weather-alerts-topic"
ALERT_QUEUE_NAME = "weather-alert-queue"
PROCESSING_QUEUE_NAME = "weather-processing-queue"

sns = boto3.client('sns', region_name=AWS_REGION)
sqs = boto3.client('sqs', region_name=AWS_REGION)


def create_topic():
    topic = sns.create_topic(Name=SNS_TOPIC_NAME)
    topic_arn = topic['TopicArn']
    print("Created SNS topic: {}".format(topic_arn))
    return topic_arn


def create_queue_subscribed_to_topic(queue_name, topic_arn, visibility_timeout, retention_seconds):
    queue = sqs.create_queue(
        QueueName=queue_name,
        Attributes={
            'VisibilityTimeout': str(visibility_timeout),
            'MessageRetentionPeriod': str(retention_seconds)
        }
    )
    queue_url = queue['QueueUrl']
    queue_arn = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=['QueueArn']
    )['Attributes']['QueueArn']

    # Allow the SNS topic to send messages to this queue
    policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Sid": "AllowSNSPublish",
            "Effect": "Allow",
            "Principal": {"Service": "sns.amazonaws.com"},
            "Action": "sqs:SendMessage",
            "Resource": queue_arn,
            "Condition": {"ArnEquals": {"aws:SourceArn": topic_arn}}
        }]
    }
    sqs.set_queue_attributes(
        QueueUrl=queue_url,
        Attributes={'Policy': json.dumps(policy)}
    )

    sns.subscribe(
        TopicArn=topic_arn,
        Protocol='sqs',
        Endpoint=queue_arn
    )

    print("Created SQS queue: {} ({})".format(queue_name, queue_url))
    print("Subscribed {} to {}".format(queue_name, topic_arn))
    return queue_url, queue_arn


if __name__ == "__main__":
    topic_arn = create_topic()

    alert_queue_url, alert_queue_arn = create_queue_subscribed_to_topic(
        ALERT_QUEUE_NAME, topic_arn, visibility_timeout=30, retention_seconds=86400
    )

    processing_queue_url, processing_queue_arn = create_queue_subscribed_to_topic(
        PROCESSING_QUEUE_NAME, topic_arn, visibility_timeout=60, retention_seconds=86400
    )

    print("\n" + "=" * 60)
    print("Paste these into config/config.py:")
    print("=" * 60)
    print('SNS_TOPIC_ARN = "{}"'.format(topic_arn))
    print('ALERT_QUEUE_URL = "{}"'.format(alert_queue_url))
    print('PROCESSING_QUEUE_URL = "{}"'.format(processing_queue_url))

    print("\n" + "=" * 60)
    print("Manual steps left to do in the AWS Console:")
    print("=" * 60)
    print("""
1. Create the alert Lambda:
   - Runtime: Python 3.12
   - Upload lambda/alert_lambda.py as the function code (handler: alert_lambda.lambda_handler)
   - Add trigger: SQS -> {alert_queue}
   - IAM role needs: AWSLambdaBasicExecutionRole + sqs:ReceiveMessage/DeleteMessage/GetQueueAttributes on {alert_arn}

2. Create the transform Lambda:
   - Runtime: Python 3.12
   - Upload lambda/transform_lambda.py as the function code (handler: transform_lambda.lambda_handler)
   - Add trigger: SQS -> {processing_queue}
   - IAM role needs: AWSLambdaBasicExecutionRole + sqs:ReceiveMessage/DeleteMessage/GetQueueAttributes on {processing_arn}
     + s3:PutObject on arn:aws:s3:::weather-kafka-pipeline-data/processed/*

3. Copy the ARNs/URLs printed above into config/config.py so the SNS publisher
   script knows where to send records.
""".format(
        alert_queue=ALERT_QUEUE_NAME,
        alert_arn=alert_queue_arn,
        processing_queue=PROCESSING_QUEUE_NAME,
        processing_arn=processing_queue_arn
    ))
