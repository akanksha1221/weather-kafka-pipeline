import json
from datetime import datetime, timezone

import boto3

S3_BUCKET = "weather-kafka-pipeline-data"

s3 = boto3.client('s3')

# Triggered by SQS (weather-processing-queue), which is subscribed to
# weather-alerts-topic (SNS). Each SQS record body is the raw SNS
# notification envelope, so the actual weather record is nested inside
# body['Message'].

def lambda_handler(event, context):
    for record in event['Records']:
        body = json.loads(record['body'])
        message = json.loads(body['Message'])
        weather = message['record']

        temp = weather.get('Temp_C')
        humidity = weather.get('Humidity_pct')

        weather['feels_like_c'] = temp - (0.4 * (temp - 10) * (1 - humidity / 100))
        weather['temp_f'] = (temp * 9 / 5) + 32
        weather['is_extreme'] = temp > 40 or temp < 0
        weather['processed_at'] = datetime.now(timezone.utc).isoformat()

        record_id = message.get('key', str(context.aws_request_id)).split('_')[-1].replace('.json', '')
        key = "processed/weather_{}.json".format(record_id)

        s3.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=json.dumps(weather)
        )
        print("Saved: s3://{}/{}".format(S3_BUCKET, key))

    return {"statusCode": 200}
