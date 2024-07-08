#!/bin/bash
if [[ $# -eq 0 ]] ; then
    : ${1? "Missing Topic name. Usage: $0 TOPIC_NAME"}
    exit 0
fi

docker exec -t kafka1 \
  kafka-topics.sh \
    --bootstrap-server kafka1:9092 \
    --create \
    --topic $1 \
    --partitions 1 \
    --replication-factor 2



    