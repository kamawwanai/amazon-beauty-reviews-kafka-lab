import json
import logging
from kafka import KafkaConsumer
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)


class BaseConsumer:
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        auto_offset_reset: str = "earliest",
    ):
        self.topic = topic
        self.group_id = group_id
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers.split(","),
            group_id=group_id,
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
            session_timeout_ms=30_000,
            heartbeat_interval_ms=10_000,
            max_poll_records=100,
        )
        logger.info(
            f"[{self.__class__.__name__}] group='{group_id}' → topic: '{topic}'"
        )

    def consume(self):
        """Генератор: (key, value) для каждого сообщения"""
        try:
            for msg in self.consumer:
                yield msg.key, msg.value
        except KafkaError as e:
            logger.error(f"Consumer error: {e}")
        finally:
            self.close()

    def close(self):
        self.consumer.close()
        logger.info(f"[{self.__class__.__name__}] closed.")
