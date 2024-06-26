from confluent_kafka.admin import AdminClient


def topic_exists(topic):
    """Checks if the given topic exists in Kafka"""
    client = AdminClient({"bootstrap.servers": "PLAINTEXT://localhost:19092"})
    topic_metadata = client.list_topics(timeout=5)
    topics = topic_metadata.topics
    return topic in topics
