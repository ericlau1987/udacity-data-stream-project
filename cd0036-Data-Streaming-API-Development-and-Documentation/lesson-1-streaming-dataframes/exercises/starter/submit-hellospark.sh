#!/bin/bash
docker exec -it cd0036-data-streaming-api-development-and-documentation-spark-1 /opt/bitnami/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 /home/workspace/lesson-1-streaming-dataframes/exercises/starter/hellospark.py | tee ../../../spark/logs/kafkaconsole.log 