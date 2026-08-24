#!/bin/bash
# Starts producer and consumer together, logging output to files

nohup python producer/kafka_producer.py > producer.log 2>&1 &
nohup python consumer/kafka_consumer.py > consumer.log 2>&1 &

echo "Pipeline running. Check producer.log and consumer.log"
