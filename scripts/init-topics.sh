#!/bin/bash
set -e

echo "Waiting for brokers..."
sleep 15

kafka-topics.sh --bootstrap-server kafka-broker-1:9092 --create --topic raw-data --replica-assignment 1:2,1:2 --config min.insync.replicas=2 --if-not-exists

kafka-topics.sh --bootstrap-server kafka-broker-1:9092 --create --topic processed-data --replica-assignment 2:1,2:1 --config min.insync.replicas=2 --if-not-exists

kafka-topics.sh --bootstrap-server kafka-broker-1:9092 --create --topic visualization-data --replica-assignment 1:2,1:2 --config min.insync.replicas=1 --if-not-exists

kafka-topics.sh --bootstrap-server kafka-broker-1:9092 --create --topic ml-results --replica-assignment 1:2,1:2 --config min.insync.replicas=2 --if-not-exists

echo ">>> All topics created!"
