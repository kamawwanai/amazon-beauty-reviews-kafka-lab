import time
import logging
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from kafka_config import BOOTSTRAP_SERVERS, TOPIC_CONFIGS

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("TopicInit")


def wait_for_brokers(bootstrap: str, retries: int = 20, delay: int = 3):
    from kafka import KafkaAdminClient
    for attempt in range(retries):
        try:
            client = KafkaAdminClient(bootstrap_servers=bootstrap)
            client.close()
            logger.info("Brokers are ready.")
            return
        except Exception:
            logger.info(f"Waiting for brokers... ({attempt + 1}/{retries})")
            time.sleep(delay)
    raise RuntimeError("Brokers did not start in time.")


def create_topics():
    wait_for_brokers(BOOTSTRAP_SERVERS)

    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    topics = [
        NewTopic(
            name=name,
            num_partitions=cfg["num_partitions"],
            replication_factor=cfg["replication_factor"],
            topic_configs=cfg["configs"],
        )
        for name, cfg in TOPIC_CONFIGS.items()
    ]

    for topic in topics:
        try:
            admin.create_topics([topic])
            logger.info(f"Created topic: {topic.name} "
                        f"(partitions={topic.num_partitions}, "
                        f"rf={topic.replication_factor}, "
                        f"isr={topic.topic_configs.get('min.insync.replicas')})")
        except TopicAlreadyExistsError:
            logger.info(f"Topic already exists: {topic.name}")

    admin.close()


if __name__ == "__main__":
    create_topics()
