import os
import sys
import logging
from collections import deque

sys.path.append("/app/producers")

from base_consumer import BaseConsumer
from base_producer import BaseProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s"
)
logger = logging.getLogger("VisualizationConsumer")

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-broker-1:9092")
INPUT_TOPIC       = "raw-data"            # читает с Broker 1
OUTPUT_TOPIC      = "visualization-data"  # пишет на Broker 1
GROUP_ID          = "visualization-group"
WINDOW_SIZE       = 500
EMIT_EVERY        = 50


# Скользящее окно

class RollingWindow:
    def __init__(self, size: int):
        self.ratings  = deque(maxlen=size)
        self.total    = 0

    def add(self, rating: int):
        self.ratings.append(rating)
        self.total += 1

    def stats(self) -> dict:
        ratings   = list(self.ratings)
        n         = len(ratings)
        avg       = round(sum(ratings) / n, 3)
        neg_share = round(sum(1 for r in ratings if r <= 2) / n * 100, 2)
        pos_share = round(sum(1 for r in ratings if r >= 4) / n * 100, 2)
        dist      = {str(i): ratings.count(i) for i in range(1, 6)}

        return {
            "total_processed": self.total,
            "window_size":     n,
            "avg_rating":      avg,
            "neg_share_pct":   neg_share,
            "pos_share_pct":   pos_share,
            "rating_dist":     dist,
        }


# Main

def main():
    consumer = BaseConsumer(BOOTSTRAP_SERVERS, INPUT_TOPIC, GROUP_ID)
    producer = BaseProducer(BOOTSTRAP_SERVERS, OUTPUT_TOPIC)
    window   = RollingWindow(WINDOW_SIZE)

    logger.info("VisualizationConsumer started")

    try:
        for key, record in consumer.consume():
            rating = int(record.get("rating", 0))
            if rating not in range(1, 6):
                continue

            window.add(rating)

            if window.total % EMIT_EVERY == 0:
                stats = window.stats()
                stats["timestamp"] = record.get("timestamp", "")
                stats["producer"]  = record.get("producer", "")

                producer.send(stats)

                logger.info(
                    f"total={stats['total_processed']:,} | "
                    f"avg={stats['avg_rating']} | "
                    f"neg={stats['neg_share_pct']}% | "
                    f"pos={stats['pos_share_pct']}%"
                )

    except KeyboardInterrupt:
        logger.info("Shutting down VisualizationConsumer")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
