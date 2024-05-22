"""Defines core consumer functionality"""
import logging

import confluent_kafka
from confluent_kafka import Consumer, OFFSET_BEGINNING
from confluent_kafka.admin import AdminClient
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen


logger = logging.getLogger(__name__)


class KafkaConsumer:
    """Defines the base kafka consumer class"""

    def __init__(
        self,
        topic_name_pattern,
        message_handler,
        is_avro=True,
        offset_earliest=True,
        sleep_secs=1.0,
        consume_timeout=1,
    ):
        """Creates a consumer object for asynchronous use"""
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest

        #
        #
        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        #
        self.broker_properties = {
                #
                # TODO
                #
            "bootstrap.servers": "PLAINTEXT://kafka0:19092",
            "group.id": f"{self.topic_name_pattern}",
            "default.topic.config":
            {"auto.offset.reset": "earliest" if offset_earliest else "latest"
            }
        }

        # TODO: Create the Consumer, using the appropriate type.
        if is_avro is True:
            # self.broker_properties["schema.registry.url"] = "http://schema-registry:8081"
            self.broker_properties["schema.registry.url"] = "http://localhost:8081"
            self.consumer = AvroConsumer(self.broker_properties)
        else:
            self.consumer = Consumer(self.broker_properties)

        #
        #
        # TODO: Configure the AvroConsumer and subscribe to the topics. Make sure to think about
        # how the `on_assign` callback should be invoked.
        #
        #
        self.consumer.subscribe([self.topic_name_pattern], on_assign=self.on_assign)

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""
        # TODO: If the topic is configured to use `offset_earliest` set the partition offset to
        # the beginning or earliest
        try:
            for partition in partitions:
                partition.offset = OFFSET_BEGINNING

            logger.info("partitions assigned for %s", self.topic_name_pattern)
            consumer.assign(partitions)
        except Exception as e:
            logger.info("on_assign is incomplete - skipping")
            logger.info(f"{e}")
            raise Exception

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
                logger.debug(f"{self.topic_name_pattern}: {num_results}")
            await gen.sleep(self.sleep_secs)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
        #
        #
        # TODO: Poll Kafka for messages. Make sure to handle any errors or exceptions.
        # Additionally, make sure you return 1 when a message is processed, and 0 when no message
        # is retrieved.
        #
        #
        while True:
            message = self.consumer.poll(timeout=self.consume_timeout)
            if message is None:
                logger.debug(f"no message received by consumer of the topic {self.topic_name_pattern}")
                return 0
            elif message.error() is not None:
                logger.info(f"error from consumer {message.error()}")
            else:
                self.message_handler(message)
                logger.INFO(f"Consumer Message Key :{message}")
                return 1 # message is processed
            sleep(self.sleep_secs)

    def _get_topics(self) -> list:
        
        client = AdminClient({"bootstrap.servers": self.broker_properties["bootstrap.servers"]})
        topic_metadata = client.list_topics(timeout=5)
        topics_included = []
        for topic in topic_metadata.topics:
            if self.topic_name_pattern in topic:
                topics_included.append(topic)
        
        return topics_included

    def close(self):
        """Cleans up any open kafka consumers"""
        #
        #
        # TODO: Cleanup the kafka consumer
        #
        #
        self.consumer.close()
