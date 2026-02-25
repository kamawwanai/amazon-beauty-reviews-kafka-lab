import json
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)


class BaseProducer:
    def __init__(self, bootstrap_servers: str, topic: str):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers.split(","),
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",           
            retries=5,
            linger_ms=10,         
            compression_type="gzip",
        )
        logger.info(f"[{self.__class__.__name__}] → topic: '{self.topic}'")

    def send(self, value: dict, key: str = None):
        try:
            future = self.producer.send(self.topic, value=value, key=key)
            future.add_errback(self._on_error)
        except KafkaError as e:
            logger.error(f"Send failed: {e}")

    def _on_error(self, exc):
        logger.error(f"Async send error: {exc}")

    def flush(self):
        self.producer.flush()

    def close(self):
        self.producer.flush()
        self.producer.close()
        logger.info(f"[{self.__class__.__name__}] closed.")
