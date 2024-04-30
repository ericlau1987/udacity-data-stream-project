# Please complete the TODO items in the code.

import asyncio
from dataclasses import dataclass, field
import json
import random

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from faker import Faker


faker = Faker()

BROKER_URL = "PLAINTEXT://kafka0:19092"
TOPIC_NAME = "org.udacity.exercise3.purchases"


@dataclass
class Purchase:
    username: str = field(default_factory=faker.user_name)
    currency: str = field(default_factory=faker.currency_code)
    amount: int = field(default_factory=lambda: random.randint(100, 200000))

    def serialize(self):
        """Serializes the object in JSON string format"""
        # TODO: Serializer the Purchase object
        #       See: https://docs.python.org/3/library/json.html#json.dumps
        return json.dumps(
            {
                "username": self.username,
                "currency": self.currency,
                "amount": self.amount,
            }
        )


async def produce_sync(topic_name):
    """Produces data synchronously into the Kafka Topic"""
    p = Producer({
        "bootstrap.servers": BROKER_URL,
        "compression.type": "lz4",
        #linger.ms is the number of milliseconds a producer is willing to wait 
        #before sending a batch out. The default value is 0, which means "send the messages right away". 
        #By introducing some lag (for example linger.ms=20 ), 
        #we increase the chances of messages being sent together in a batch.
        "linger.ms": 10000, 
        "batch.num.messages": 10000,
        "queue.buffering.max.messages": 100000,
        "queue.buffering.max.kbytes": 100000
    
    })

    # TODO: Write a synchronous production loop.
    #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.Producer.flush
    while True:
        # TODO: Instantiate a `Purchase` on every iteration. Make sure to serialize it before
        #       sending it to Kafka!
        # Do not delete this!
        p.produce(topic_name, Purchase().serialize())
        await asyncio.sleep(0.01)

        
def main():
    """Checks for topic and creates the topic if it does not exist"""
    create_topic(TOPIC_NAME)
    try:
        asyncio.run(produce_consume())
    except KeyboardInterrupt as e:
        print("shutting down")

    
async def produce_consume():
    """Runs the Producer and Consumer tasks"""
    t1 = asyncio.create_task(produce_sync(TOPIC_NAME))
    t2 = asyncio.create_task(_consume(TOPIC_NAME))
    await t1
    await t2

    
async def _consume(topic_name):
    """Consumes produced messages"""
    c = Consumer({"bootstrap.servers": BROKER_URL, "group.id": "0"})
    c.subscribe([topic_name])
    num_consumed=0
    while True:
        msg = c.consume(timeout=0.001)
        if msg:
            num_consumed += 1
            if num_consumed % 100 == 0:
                print(f"consumed {num_consumed} messages")
        else:
            await asyncio.sleep(0.01)

        
def create_topic(client):
    """Creates the topic with the given topic name"""
    client = AdminClient({"bootstrap.servers": BROKER_URL})
    futures = client.create_topics(
        [NewTopic(topic=TOPIC_NAME, num_partitions=5, replication_factor=1)]
    )
    for _, future in futures.items():
        try:
            future.result()
        except Exception as e:
            print("exiting production loop")


if __name__ == "__main__":
    main()