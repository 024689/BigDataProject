#!/usr/bin/env bash

docker-compose up -d -f ./airflow/docker-compose.yaml
docker-compose up -d -f ./jobs/virsualisation/docker-compose.yaml

echo "Airflow and elk ready to use"