# Weather Kafka Data Engineering Pipeline

A real-time data pipeline that streams weather readings through Apache Kafka
and lands them in S3 as JSON, ready for Glue + Athena analysis.

## Architecture

```
📄 CSV Dataset → 🐍 Python Producer → 🔀 Apache Kafka → 🐍 Python Consumer → 🪣 AWS S3
                                        (Docker)                                 │
                                                                                  ▼
                                                                       🕷️  AWS Glue Crawler
                                                                                  │
                                                                                  ▼
                                                                       📚 AWS Glue Catalog
                                                                                  │
                                                                                  ▼
                                                                       🔎 Amazon Athena
                                                                          (SQL Query Engine)
```

## Tech Stack

- Python 3.12
- Apache Kafka (via Docker)
- kafka-python-ng
- pandas
- AWS S3
- AWS Glue
- Amazon Athena
- Docker & Docker Compose

```
weather_kafka_pipeline/
├── data/
│   └── weather_data.csv          # sample dataset (500 rows)
├── producer/
│   └── kafka_producer.py         # reads CSV, sends one random row/sec to Kafka
├── consumer/
│   └── kafka_consumer.py         # reads from Kafka, saves each msg as JSON to S3
├── config/
│   └── config.py                 # all settings in one place (broker IP, topic, S3 bucket)
├── docker-compose.yml            # local Kafka + Zookeeper + Kafka UI
├── requirements.txt              # kafka-python, boto3, pandas, s3fs
├── start.sh                      # one script to start producer and consumer together
└── README.md
```

## How it works

1. `kafka_producer.py` reads `data/weather_data.csv` with pandas, picks one
   random row per second, and publishes it as JSON to the `weather_stream`
   Kafka topic.
2. `kafka_consumer.py` subscribes to `weather_stream` and writes each message
   as its own JSON file to `s3://<bucket>/weather_data/weather_<n>.json`.
3. AWS Glue can crawl that S3 prefix and Athena can query the resulting table.

All settings (broker address, topic name, S3 bucket, region) live in
[`config/config.py`](config/config.py) — that's the only file you need to edit.

## Local setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Start Kafka locally with Docker

```bash
docker-compose up -d
```

This starts Zookeeper, a Kafka broker (advertised on `localhost:9092`), and
Kafka UI at [http://localhost:8080](http://localhost:8080).

### 3. Create the Kafka topic

```bash
docker exec -it kafka kafka-topics --create --topic weather_stream --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### 4. Configure AWS credentials

The consumer needs AWS credentials to write to S3:

```bash
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=us-east-1
```

### 5. Run the producer

```bash
python producer/kafka_producer.py
```

### 6. Run the consumer (in a separate terminal)

```bash
python consumer/kafka_consumer.py
```

### Or start both at once

```bash
./start.sh
```

Logs are written to `producer.log` and `consumer.log`.

## AWS setup (running on EC2 / real S3)

1. **Create an S3 bucket** and note its name (e.g. `my-weather-kafka-bucket`).
   Update `S3_BUCKET` in [`config/config.py`](config/config.py) to match.

2. **Set AWS credentials.** Either:
   - Attach an IAM role with S3 write permissions to your EC2 instance (recommended), or
   - Export credentials in the shell before running the consumer:
     ```bash
     export AWS_ACCESS_KEY_ID=your_access_key
     export AWS_SECRET_ACCESS_KEY=your_secret_key
     ```

3. **On EC2:** update `KAFKA_BROKER` in `config/config.py` to the EC2 instance's
   public IP, e.g.:
   ```python
   KAFKA_BROKER = "3.91.12.45:9092"
   ```
   Also update `KAFKA_ADVERTISED_LISTENERS` in `docker-compose.yml` to use that
   public IP instead of `localhost` so remote producers/consumers can connect.

4. **Create the Kafka topic** on the broker:
   ```bash
   bin/kafka-topics.sh --create --topic weather_stream --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
   ```

5. **Set up a Glue Crawler** pointing at `s3://MY_BUCKET_NAME/weather_data/`:
   - AWS Console → Glue → Crawlers → Create crawler
   - Data source: S3 path `s3://MY_BUCKET_NAME/weather_data/`
   - IAM role: one with `AWSGlueServiceRole` + S3 read access
   - Target database: create/select a Glue database (e.g. `weather_db`)
   - Run the crawler to infer the schema and create a table (e.g. `weather_data`)

6. **Sample Athena query** (after the crawler has run):
   ```sql
   SELECT city, country, temp_c, humidity_pct, condition, date
   FROM weather_db.weather_data
   WHERE temp_c > 30
   ORDER BY date DESC
   LIMIT 20;
   ```

## Verifying the pipeline is flowing

- **Kafka UI:** open [http://localhost:8080](http://localhost:8080), select the
  `local` cluster, open the `weather_stream` topic, and watch messages arrive.
- **Producer/consumer console output:** both scripts print each
  message they send/save.
- **S3:** check the bucket for new JSON files:
  ```bash
  aws s3 ls s3://MY_BUCKET_NAME/weather_data/ --recursive
  ```
## Live API

Base URL: https://YOUR-API-ID.execute-api.us-east-1.amazonaws.com

GET /weather              — latest weather records
GET /weather?city=Mumbai  — filter by city
GET /weather/summary      — total records count

## Enhanced Architecture (SNS + SQS + Lambda)

```
CSV → Producer → Kafka → Consumer → S3 (weather_data/)
                    │
                    └────────────→ SNS publisher → SNS Topic (weather-alerts-topic)
                                                        │
                                          ┌─────────────┴─────────────┐
                                          ▼                           ▼
                              SQS (weather-alert-queue)   SQS (weather-processing-queue)
                                          │                           │
                                          ▼                           ▼
                                Lambda (alert_lambda)       Lambda (transform_lambda)
                                          │                           │
                                    (logs alert)              S3 (processed/)
                                                                      │
                                                                      ▼
                                                      API Gateway → Lambda (weather-api)
```

New pieces, on top of the original pipeline:

- [`consumer/sns_publisher.py`](consumer/sns_publisher.py) — a second Kafka
  consumer (separate consumer group, same `weather_stream` topic) that
  publishes every record to an SNS topic. Runs alongside `kafka_consumer.py`
  without touching it.
- [`lambda/alert_lambda.py`](lambda/alert_lambda.py) — subscribed to
  `weather-alert-queue`. Logs an alert when `Temp_C > 40` or `Temp_C < 0`.
- [`lambda/transform_lambda.py`](lambda/transform_lambda.py) — subscribed to
  `weather-processing-queue`. Enriches each record (`feels_like_c`, `temp_f`,
  `is_extreme`, `processed_at`) and saves it to `s3://weather-kafka-pipeline-data/processed/`.
- [`infrastructure/setup_aws.py`](infrastructure/setup_aws.py) — creates the
  SNS topic and both SQS queues (with subscriptions, visibility timeouts, and
  retention) and prints the ARNs/URLs to paste into `config/config.py`.

### Run order

```bash
# 1. Start Kafka locally
docker-compose up -d

# 2. Provision SNS + SQS (one-time)
python infrastructure/setup_aws.py
# → paste the printed SNS_TOPIC_ARN / ALERT_QUEUE_URL / PROCESSING_QUEUE_URL into config/config.py

# 3. In the AWS Console: create alert_lambda and transform_lambda, wire their
#    SQS triggers, and attach IAM permissions (setup_aws.py prints the exact steps)

# 4. Run the existing pipeline
python producer/kafka_producer.py       # terminal 1
python consumer/kafka_consumer.py       # terminal 2 (unchanged — writes to S3)

# 5. Run the new SNS publisher alongside it
python consumer/sns_publisher.py        # terminal 3 (fans records out to SNS)
```

From here, every record flows: Kafka → S3 (as before) and Kafka → SNS → SQS →
Lambda (alerts + transform), with the transformed/enriched output landing in
`s3://weather-kafka-pipeline-data/processed/`.

## Monitoring

CloudWatch Dashboard:
https://console.aws.amazon.com/cloudwatch/home#dashboards:name=WeatherKafkaDashboard

Alarms:
- `weather-api-errors`: fires if API Lambda errors >= 3 in 5 min
- `processing-queue-depth`: fires if SQS backlog >= 100 msgs
- `transform-lambda-slow`: fires if transform takes > 10 seconds

Custom Metrics (Namespace: `WeatherPipeline`):
- `S3FileCount`: total JSON files in S3
- `ConsumerThroughput`: messages saved per minute

All of this is a monitoring layer added on top of the existing code —
`kafka_consumer.py` and the Lambda functions were left untouched:

- [`monitoring/cloudwatch_setup.py`](monitoring/cloudwatch_setup.py) — creates
  the dashboard and the 3 alarms above (run once)
- [`monitoring/push_metrics.py`](monitoring/push_metrics.py) — helper
  functions that publish the two custom metrics to CloudWatch
- [`monitoring/s3_metrics_poller.py`](monitoring/s3_metrics_poller.py) — a
  standalone script that polls the S3 bucket every 60s and calls
  `push_metrics.py` (runs independently, doesn't call into the consumer)
- [`monitoring/log_insights_queries.py`](monitoring/log_insights_queries.py) —
  reference Logs Insights queries; run it to print them. Note: queries 1, 2
  and 4 assume structured JSON logs (`{"event": ..., "data": ...}`), which
  weren't added to the Lambdas — only query 3 (error analysis) works as-is
  against their current plain-text logs.

Function names in the dashboard/alarms come from `config/config.py`
(`API_LAMBDA_NAME`, `ALERT_LAMBDA_NAME`, `TRANSFORM_LAMBDA_NAME`) — update
those to match whatever you actually named the Lambdas in the console.