"""Producer base-class providing common utilites and functionality"""
import logging
# import logging.config
import time
from pathlib import Path

from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

# logging.config.fileConfig(f"{Path(__file__).parents[0]}/logging.ini")
logger = logging.getLogger(__name__)

class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])
    BROKER_URL = "PLAINTEXT://localhost:19092" # "PLAINTEXT://kafka0:19092"
    SCHEMA_REGISTRY_URL = "http://localhost:8081/" #"http://schema-registry:8081/"

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=40,
        num_replicas=1,
        topic_created_only=False
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        #
        #
        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry! -- Done
        #
        #
        self.broker_properties = {
            # TODO
            "bootstrap.servers" : Producer.BROKER_URL, 
            # TODO
            # "schema.registry.url" : "http://localhost:8081",
            "schema.registry.url" : Producer.SCHEMA_REGISTRY_URL, #--> Docker
            # TODO
            "default.topic.config": {"acks": "all"}
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            logger.info(f"The topic {self.topic_name} is being created.")
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)
        else:
            logger.info(f"The topic {self.topic_name} exists and skipping creating the topic.")

        if not topic_created_only:
        # TODO: Configure the AvroProducer - Done
            self.producer = AvroProducer(
                self.broker_properties,
                default_key_schema = self.key_schema,
                default_value_schema=self.value_schema
            )
        else:
            pass

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        #
        #
        # TODO: Write code that creates the topic for this producer if it does not already exist on
        # the Kafka Broker. --Done
        #
        #
        client = AdminClient({"bootstrap.servers": self.broker_properties["bootstrap.servers"]})

        if not self.topic_exists(client):
            logger.debug(f"The topic {self.topic_name} doesn't exist.")
            futures = client.create_topics(
            [
                NewTopic(
                    topic=self.topic_name,
                    num_partitions=self.num_partitions,
                    replication_factor = self.num_replicas,
                    config = {'cleanup.policy' : 'compact',
                        'compression.type' : 'lz4', 
                        'delete.retention.ms' : 2000, 
                        'file.delete.delay.ms' : 20000}
                    )
                ]
            )
            for topic, future in futures.items():
                try:
                    future.result()
                    logger.info(f"The topic {self.topic_name} is created.")
                except Exception as e:
                    logger.info("topic creation kafka integration incomplete - skipping")
                    raise Exception (f"Failed to create the topic {self.topic_name}! {e}")       

        else:
            logger.info(f"The topic {self.topic_name} exists and skipping creating the topic.")

    def topic_exists(self, client) -> bool:
        """Checks if the given topic exists"""
        logger.debug(f"checking whether the topic {self.topic_name} exists or not.")
        topic_metadata = client.list_topics(timeout=5)
        return topic_metadata.topics.get(self.topic_name) is not None

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        #
        #
        # TODO: Write cleanup code for the Producer here
        #
        #
        if self.topic_name is not None:
            self.producer.flush()
            logger.info("Producer is cleaned up.")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
