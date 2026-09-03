import json
import os
import sys

import boto3

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import (
    AWS_REGION, SNS_TOPIC_ARN, CLOUDWATCH_NAMESPACE, DASHBOARD_NAME,
    API_LAMBDA_NAME, ALERT_LAMBDA_NAME, TRANSFORM_LAMBDA_NAME
)

ALERT_QUEUE_NAME = "weather-alert-queue"
PROCESSING_QUEUE_NAME = "weather-processing-queue"
SNS_TOPIC_NAME = "weather-alerts-topic"

cloudwatch = boto3.client('cloudwatch', region_name=AWS_REGION)


def lambda_metric(metric_name, function_name, stat=None):
    metric = ["AWS/Lambda", metric_name, "FunctionName", function_name]
    if stat:
        metric.append({"stat": stat})
    return metric


def build_dashboard_body():
    widgets = [
        {
            "type": "metric", "x": 0, "y": 0, "width": 12, "height": 6,
            "properties": {
                "title": "Lambda Invocations",
                "view": "timeSeries",
                "region": AWS_REGION,
                "period": 300,
                "metrics": [
                    lambda_metric("Invocations", API_LAMBDA_NAME),
                    lambda_metric("Invocations", ALERT_LAMBDA_NAME),
                    lambda_metric("Invocations", TRANSFORM_LAMBDA_NAME),
                ]
            }
        },
        {
            "type": "metric", "x": 12, "y": 0, "width": 12, "height": 6,
            "properties": {
                "title": "Lambda Errors",
                "view": "timeSeries",
                "region": AWS_REGION,
                "period": 300,
                "metrics": [
                    lambda_metric("Errors", API_LAMBDA_NAME),
                    lambda_metric("Errors", ALERT_LAMBDA_NAME),
                    lambda_metric("Errors", TRANSFORM_LAMBDA_NAME),
                ]
            }
        },
        {
            "type": "metric", "x": 0, "y": 6, "width": 12, "height": 6,
            "properties": {
                "title": "Lambda Duration (ms)",
                "view": "timeSeries",
                "region": AWS_REGION,
                "period": 300,
                "metrics": [
                    lambda_metric("Duration", API_LAMBDA_NAME, stat="Average"),
                    lambda_metric("Duration", ALERT_LAMBDA_NAME, stat="Average"),
                    lambda_metric("Duration", TRANSFORM_LAMBDA_NAME, stat="Average"),
                ]
            }
        },
        {
            "type": "metric", "x": 12, "y": 6, "width": 12, "height": 6,
            "properties": {
                "title": "SQS Queue Depth",
                "view": "timeSeries",
                "region": AWS_REGION,
                "period": 60,
                "metrics": [
                    ["AWS/SQS", "ApproximateNumberOfMessagesVisible", "QueueName", ALERT_QUEUE_NAME],
                    ["AWS/SQS", "ApproximateNumberOfMessagesVisible", "QueueName", PROCESSING_QUEUE_NAME],
                ]
            }
        },
        {
            "type": "metric", "x": 0, "y": 12, "width": 12, "height": 6,
            "properties": {
                "title": "SNS Publishes",
                "view": "timeSeries",
                "region": AWS_REGION,
                "period": 300,
                "metrics": [
                    ["AWS/SNS", "NumberOfMessagesPublished", "TopicName", SNS_TOPIC_NAME],
                ]
            }
        },
        {
            "type": "metric", "x": 12, "y": 12, "width": 6, "height": 6,
            "properties": {
                "title": "Total S3 Files",
                "view": "singleValue",
                "region": AWS_REGION,
                "metrics": [
                    [CLOUDWATCH_NAMESPACE, "S3FileCount"],
                ]
            }
        },
    ]
    return json.dumps({"widgets": widgets})


def create_dashboard():
    cloudwatch.put_dashboard(
        DashboardName=DASHBOARD_NAME,
        DashboardBody=build_dashboard_body()
    )
    print("Created dashboard: {}".format(DASHBOARD_NAME))


def create_alarms():
    cloudwatch.put_metric_alarm(
        AlarmName="weather-api-errors",
        MetricName="Errors",
        Namespace="AWS/Lambda",
        Dimensions=[{"Name": "FunctionName", "Value": API_LAMBDA_NAME}],
        Statistic="Sum",
        Period=300,
        EvaluationPeriods=1,
        Threshold=3,
        ComparisonOperator="GreaterThanOrEqualToThreshold",
        AlarmActions=[SNS_TOPIC_ARN],
        TreatMissingData="notBreaching"
    )
    print("Created alarm: weather-api-errors")

    cloudwatch.put_metric_alarm(
        AlarmName="processing-queue-depth",
        MetricName="ApproximateNumberOfMessagesVisible",
        Namespace="AWS/SQS",
        Dimensions=[{"Name": "QueueName", "Value": PROCESSING_QUEUE_NAME}],
        Statistic="Average",
        Period=300,
        EvaluationPeriods=1,
        Threshold=100,
        ComparisonOperator="GreaterThanOrEqualToThreshold",
        AlarmActions=[SNS_TOPIC_ARN],
        TreatMissingData="notBreaching"
    )
    print("Created alarm: processing-queue-depth")

    cloudwatch.put_metric_alarm(
        AlarmName="transform-lambda-slow",
        MetricName="Duration",
        Namespace="AWS/Lambda",
        Dimensions=[{"Name": "FunctionName", "Value": TRANSFORM_LAMBDA_NAME}],
        Statistic="Average",
        Period=300,
        EvaluationPeriods=1,
        Threshold=10000,
        ComparisonOperator="GreaterThanOrEqualToThreshold",
        AlarmActions=[SNS_TOPIC_ARN],
        TreatMissingData="notBreaching"
    )
    print("Created alarm: transform-lambda-slow")


if __name__ == "__main__":
    create_dashboard()
    create_alarms()

    print("\nDashboard URL:")
    print("https://console.aws.amazon.com/cloudwatch/home#dashboards:name={}".format(DASHBOARD_NAME))
