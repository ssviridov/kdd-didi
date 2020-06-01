#!/bin/sh

HOST="localhost"

echo "INFO: Compose build" && \
docker-compose build --build-arg HOST=${HOST} && \
echo "INFO: Compose up" && \
docker-compose up -d