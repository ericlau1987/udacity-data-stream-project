"""Configures a Kafka Connector for Postgres Station data"""
import json
import logging
import logging.config

from confluent_kafka.admin import AdminClient, NewTopic
from kafka.admin.new_partitions import NewPartitions
from pathlib import Path

import requests

logging.config.fileConfig(f"{Path(__file__).parents[0]}/logging.ini")
logger = logging.getLogger(__name__)



KAFKA_CONNECT_URL = "http://connect:8083/connectors"
CONNECTOR_NAME = "stations"

def configure_connector():
    # create_topic(topic_name, num_partitions=40, num_replicas=1)
    # logger.debug(f"creating the topic {topic_name}")
    """Starts and configures the Kafka Connect connector"""
    logger.debug("creating or updating kafka connect connector...")

    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        logger.debug("connector already created skipping recreation")
        return

    # TODO: Complete the Kafka Connect Config below.
    # Directions: Use the JDBC Source Connector to connect to Postgres. Load the `stations` table
    # using incrementing mode, with `stop_id` as the incrementing column name.
    # Make sure to think about what an appropriate topic prefix would be, and how frequently Kafka
    # Connect should run this connector (hint: not very often!)
    resp = requests.post(
       KAFKA_CONNECT_URL,
       headers={"Content-Type": "application/json"},
       data=json.dumps({
           "name": CONNECTOR_NAME,
           "config": {
               "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
               "key.converter": "org.apache.kafka.connect.json.JsonConverter",
               "key.converter.schemas.enable": "false",
               "value.converter": "org.apache.kafka.connect.json.JsonConverter",
               "value.converter.schemas.enable": "false",
               "batch.max.rows": "5000",
               # TODO
               "connection.url": "jdbc:postgresql://postgres:5432/cta",
               # TODO
               "connection.user": "cta_admin",
               # TODO
               "connection.password": "chicago",
               # TODO
               "table.whitelist": "stations",
               # TODO
               "mode": "incrementing",
               # TODO
               "incrementing.column.name": "stop_id",
               "topic.prefix": "org.chicago.cta.",
               # TODO
               "poll.interval.ms": "100000",
               "topic.creation.enable": True,
               "topic.creation.groups": True,
               "topic.creation.default.replication.factor": 1,
               "topic.creation.default.partitions": 40
           }
       }),
    )

    ## Ensure a healthy response was given
    try:
        resp.raise_for_status()
        logger.debug("connector created successfully")
    except Exception as e:
        logger.info(f"Failed to set up postgres SQL database cta")
        logger.info(f"{e}")
        raise Exception


if __name__ == "__main__":
    configure_connector()
