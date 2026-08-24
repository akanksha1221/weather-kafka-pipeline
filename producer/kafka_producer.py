import os
import sys
import json
from time import sleep

import pandas as pd
from kafka import KafkaProducer

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import KAFKA_BROKER, KAFKA_TOPIC, CSV_PATH

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

df = pd.read_csv(CSV_PATH)

if __name__ == "__main__":
    while True:
        dict_weather = df.sample(1).to_dict(orient="records")[0]
        producer.send(KAFKA_TOPIC, value=dict_weather)
        print(dict_weather)
        sleep(1)
