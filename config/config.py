# All settings in one place — user only edits this file

KAFKA_BROKER = "localhost:9092"    # change to EC2 public IP for cloud, e.g. "3.91.12.45:9092"
KAFKA_TOPIC = "weather_stream"
S3_BUCKET = "weather-kafka-pipeline-data"   # change to your real bucket name
AWS_REGION = "ap-south-1"

CSV_PATH = "data/weather_data.csv"
S3_PREFIX = "weather_data"

# config/config.py — add this line
API_URL = 'https://YOUR-API-ID.execute-api.us-east-1.amazonaws.com'
