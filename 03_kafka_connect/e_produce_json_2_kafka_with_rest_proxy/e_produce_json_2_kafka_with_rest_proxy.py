# Please complete TODO items in the code

from dataclasses import asdict, dataclass, field
import json
import time
import random

import requests
from confluent_kafka import avro, Consumer, Producer
from confluent_kafka.avro import AvroConsumer, AvroProducer, CachedSchemaRegistryClient
from confluent_kafka.admin import AdminClient, NewTopic
from faker import Faker


faker = Faker()
REST_PROXY_URL = "http://rest-proxy:8082"
BROKER_URL = "PLAINTEXT://kafka0:19092"


def topic_exists(client, topic_name):
    """Checks if the given topic exists"""
    # TODO: Check to see if the given topic exists
    #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.Consumer.list_topics
    topic_metadata = client.list_topics(timeout=5)
    return topic_metadata.topics.get(topic_name) is not None


def create_topic(client, topic_name):
    """Creates the topic with the given topic name"""
    # TODO: Create the topic. Make sure to set the topic name, the number of partitions, the
    # replication factor. Additionally, set the config to have a cleanup policy of delete, a
    # compression type of lz4, delete retention milliseconds of 2 seconds, and a file delete delay
    # milliseconds of 2 second.
    #
    # See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.admin.NewTopic
    # See: https://docs.confluent.io/current/installation/configuration/topic-configs.html
    futures = client.create_topics(
        [
            NewTopic(
               topic=topic_name,
               num_partitions=5,
               replication_factor=1,
               config={
                "cleanup.policy": "compact",
                "compression.type": "lz4",
                "delete.retention.ms": 100,
                "file.delete.delay.ms": 100
               }
            )
        ]
    )

    for topic, future in futures.items():
        try:
            future.result()
            print("topic created")
        except Exception as e:
            print(f"failed to create topic {topic_name}: {e}")
            raise


def produce(topic_name):
    """Produces data using REST Proxy"""

    # TODO: Set the appropriate headers
    #       See: https://docs.confluent.io/current/kafka-rest/api.html#content-types
    headers = {
       "Content-Type": "application/vnd.kafka.json.v2+json"
    }
    # TODO: Define the JSON Payload to b sent to REST Proxy
    #       To create data, use `asdict(ClickEvent())`
    #       See: https://docs.confluent.io/current/kafka-rest/api.html#post--topics-(string-topic_name)
    data = {
        "records" : [
            {"value": asdict(ClickEvent())}
        ]
    }
    # TODO: What URL should be used?
    #       See: https://docs.confluent.io/current/kafka-rest/api.html#post--topics-(string-topic_name)
    resp = requests.post(
        f"{REST_PROXY_URL}/topics/{topic_name}", data=json.dumps(data), headers=headers  # TODO
    )

    try:
        resp.raise_for_status()
    except:
        print(f"Failed to send data to REST Proxy {json.dumps(resp.json(), indent=2)}")

    print(f"Sent data to REST Proxy {json.dumps(resp.json(), indent=2)}")


@dataclass
class ClickEvent:
    email: str = field(default_factory=faker.email)
    timestamp: str = field(default_factory=faker.iso8601)
    uri: str = field(default_factory=faker.uri)
    number: int = field(default_factory=lambda: random.randint(0, 999))


def main():
    client = AdminClient({"bootstrap.servers": BROKER_URL})
    topic_name = "lesson4.solution5.click_events"

    #
    # TODO: Decide on a topic name
    #
    # topic_name = "lesson4.sample5"
    # if topic_exists(client, topic_name):
    #     pass 
    # else:
    #     create_topic(client, topic_name)

    """Runs the simulation against REST Proxy"""
    try:
        while True:
            produce(topic_name)
            time.sleep(0.5)
    except KeyboardInterrupt as e:
        print("shutting down")


if __name__ == "__main__":
    main()
