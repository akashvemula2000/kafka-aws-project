#!/bin/bash

# Kafka broker connection details
BROKER="localhost:9092"

# Topic list
TOPICS=("vehicle_data" "gps_data" "traffic_data" "weather_data" "emergency_data")

# Loop through topics
for TOPIC in "${TOPICS[@]}"
do
  echo "Deleting topic: $TOPIC"
  docker exec -it kafka kafka-topics.sh --delete --bootstrap-server $BROKER --topic $TOPIC

  echo "Recreating topic: $TOPIC"
  docker exec -it kafka kafka-topics.sh --create --bootstrap-server $BROKER --topic $TOPIC --partitions 1 --replication-factor 1
done

echo "All topics reset successfully."