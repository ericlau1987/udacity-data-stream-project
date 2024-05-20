# Please complete the TODO items in the code

import asyncio
from dataclasses import dataclass, field
import json
import random

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from faker import Faker


faker = Faker()

BROKER_URL = "PLAINTEXT://kafka0:19092"
TOPIC_NAME = "org.chicago.cta.weather.v1"
offset_earliest = True
broker_properties = {
                #
                # TODO
                #
            "bootstrap.servers": "PLAINTEXT://kafka0:19092",
            "group.id": f"{TOPIC_NAME}",
            "default.topic.config":
            {"auto.offset.reset": "earliest" if offset_earliest else "latest"}
        }

async def consume(topic_name):
    """Consumes data from the Kafka Topic"""
    c = Consumer(broker_properties)
    c.subscribe([TOPIC_NAME])

    while True:
        #
        # TODO: Write a loop that uses consume to grab 5 messages at a time and has a timeout.
        #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/index.html?highlight=partition#confluent_kafka.Consumer.consume
        messages = c.poll(0.1) # capture 10 message for each time
        # print(f"consumed {len(messages)} messages.")
        # TODO: Print something to indicate how many messages you've consumed. Print the key and value of
        #       any message(s) you consumed
        print(messages if not messages else messages.value())
        if not messages:
            print("no messages detected and continue the loop")
            continue
        for message in messages:
            if message is None:
                print("no message received by consumer")
            elif message.error() is not None:
                print(f"error from consumer {message.error()}")
            else:
                print(f"consumed message {message.key()}: {message.value()}")

        # Do not delete this!
        await asyncio.sleep(0.01)

def main():
    """Checks for topic and creates the topic if it does not exist"""
    # client = AdminClient(broker_properties)
    # consumer = Consumer(broker_properties)

    try:
        # create_topic(client, TOPIC_NAME)
        asyncio.run(consume(TOPIC_NAME))
    except KeyboardInterrupt as e:
        print("shutting down")


async def produce(topic_name):
    """Produces data into the Kafka Topic"""
    p = Producer({"bootstrap.servers": BROKER_URL})
    while True:
        for _ in range(10):
            p.produce(topic_name, Purchase().serialize())
        await asyncio.sleep(0.01)


async def produce_consume(topic_name):
    """Runs the Producer and Consumer tasks"""
    t1 = asyncio.create_task(produce(topic_name))
    t2 = asyncio.create_task(consume(topic_name))
    await t1
    await t2


@dataclass
class Purchase:
    username: str = field(default_factory=faker.user_name)
    currency: str = field(default_factory=faker.currency_code)
    amount: int = field(default_factory=lambda: random.randint(100, 200000))

    def serialize(self):
        return json.dumps(
            {
                "username": self.username,
                "currency": self.currency,
                "amount": self.amount,
            }
        )


if __name__ == "__main__":
    main()