# All settings in one place — user only edits this file

KAFKA_BROKER = "localhost:9092"    # change to EC2 public IP for cloud, e.g. "3.91.12.45:9092"
KAFKA_TOPIC = "weather_stream"
S3_BUCKET = "weather-kafka-pipeline-data"   # change to your real bucket name
AWS_REGION = "ap-south-1"

CSV_PATH = "data/weather_data.csv"
S3_PREFIX = "weather_data"

# config/config.py — add this line
API_URL = 'https://YOUR-API-ID.execute-api.us-east-1.amazonaws.com'

# SNS/SQS settings — paste values printed by infrastructure/setup_aws.py
SNS_TOPIC_ARN = "arn:aws:sns:ap-south-1:294960493171:weather-alerts-topic"
ALERT_QUEUE_URL = "https://sqs.ap-south-1.amazonaws.com/294960493171/weather-alert-queue"
PROCESSING_QUEUE_URL = "https://sqs.ap-south-1.amazonaws.com/294960493171/weather-processing-queue"

# CloudWatch monitoring settings
CLOUDWATCH_NAMESPACE = 'WeatherPipeline'
DASHBOARD_NAME       = 'WeatherKafkaDashboard'
API_LAMBDA_NAME       = 'weather-api'        # rename to match your deployed function
ALERT_LAMBDA_NAME     = 'alert-lambda'       # rename to match your deployed function
TRANSFORM_LAMBDA_NAME = 'transform-lambda'   # rename to match your deployed function
