import json

# Triggered by SQS (weather-alert-queue), which is subscribed to
# weather-alerts-topic (SNS). Each SQS record body is the raw SNS
# notification envelope, so the actual weather record is nested inside
# body['Message'].

def lambda_handler(event, context):
    for record in event['Records']:
        body = json.loads(record['body'])
        message = json.loads(body['Message'])
        weather = message['record']

        city = weather.get('City')
        temp = weather.get('Temp_C')
        condition = weather.get('Condition')
        timestamp = weather.get('Date')

        if temp > 40 or temp < 0:
            print("ALERT: Extreme temperature detected!")
            print("City: {}, Temp: {}°C, Condition: {}".format(city, temp, condition))
            print("Time: {}".format(timestamp))

    return {"statusCode": 200}
