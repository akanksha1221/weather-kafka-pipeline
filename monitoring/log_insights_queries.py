# Useful CloudWatch Logs Insights queries for this pipeline.
#
# NOTE: alert_lambda.py and transform_lambda.py were left untouched (no
# structured JSON logging was added), so their logs are plain print() text,
# not the {"event": ..., "data": ...} shape these queries assume. Queries 1,
# 2 and 4 below are written for that structured shape and will need
# structured logging added to the Lambdas before they return useful results.
# Query 3 (error analysis) works against plain-text logs as-is.

QUERY_1_EXTREME_TEMP_ALERTS = """
fields @timestamp, data.city, data.temp
| filter event = "alert_triggered"
| sort @timestamp desc
| limit 20
"""

QUERY_2_AVG_PROCESSING_TIME_PER_CITY = """
fields @timestamp, data.city, @duration
| filter event = "transform_complete"
| stats avg(@duration) by data.city
"""

QUERY_3_ERROR_ANALYSIS = """
fields @timestamp, @message
| filter @message like /ERROR/
| sort @timestamp desc
| limit 50
"""

QUERY_4_CONSUMER_THROUGHPUT_OVER_TIME = """
fields @timestamp, data.count
| filter event = "weather_record_saved"
| stats count() by bin(5m)
"""

if __name__ == "__main__":
    print("Query 1 — Extreme temperature alerts (log group: /aws/lambda/alert-lambda):")
    print(QUERY_1_EXTREME_TEMP_ALERTS)

    print("Query 2 — Average processing time per city (log group: /aws/lambda/transform-lambda):")
    print(QUERY_2_AVG_PROCESSING_TIME_PER_CITY)

    print("Query 3 — Error analysis (any Lambda log group):")
    print(QUERY_3_ERROR_ANALYSIS)

    print("Query 4 — Consumer throughput over time (log group: /aws/lambda/alert-lambda or transform-lambda):")
    print(QUERY_4_CONSUMER_THROUGHPUT_OVER_TIME)
