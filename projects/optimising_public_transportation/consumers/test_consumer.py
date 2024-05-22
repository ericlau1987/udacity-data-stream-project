import asyncio
from time import sleep

from confluent_kafka import Consumer, Producer, OFFSET_BEGINNING
from confluent_kafka.admin import AdminClient, NewTopic

import logging
import logging.config
from pathlib import Path

BROKER_URL = "PLAINTEXT://kafka0:19092"
logger = logging.getLogger(__name__)
logging.config.fileConfig(f"{Path(__file__).parents[0]}/logging.ini")


def consume(topic_name):
    """Consumes data from the Kafka Topic"""
    # Sleep for a few seconds to give the producer time to create some data
    sleep(2.5)

    # TODO: Set the offset reset to earliest
    c = Consumer(
        {
            "bootstrap.servers": BROKER_URL,
            "group.id": "0",
            "auto.offset.reset": "earliest",
            # "max.poll.interval.ms": 500,
            # "session.timeout.ms": 600
        }
    )

    # TODO: Configure the on_assign callback
    c.subscribe([topic_name], on_assign=on_assign)

    while True:
        message = c.poll(40)
        if message is None:
            print("no message received by consumer")
        elif message.error() is not None:
            print(f"error from consumer {message.error()}")
        else:
            print(f"consumed message {message.key()}: {message.value()}")
        sleep(0.1)
        
def on_assign(consumer, partitions):
    """Callback for when topic assignment takes place"""
    # TODO: If the topic is configured to use `offset_earliest` set the partition offset to
    # the beginning or earliest
    for partition in partitions:
        partition.offset = OFFSET_BEGINNING

    # TODO: Assign the consumer the partitions
    consumer.assign(partitions)
      

if __name__ == "__main__":
    try:
        consume('org.chicago.cta.stations.table')
    except KeyboardInterrupt:
        logger.debug("exit!!!")