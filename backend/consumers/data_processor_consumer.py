import os
import re
import logging
import sys

sys.path.append("/app/producers")  # для импорта BaseProducer

from base_consumer import BaseConsumer
from base_producer import BaseProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s"
)
logger = logging.getLogger("DataProcessorConsumer")

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-broker-1:9092")
INPUT_TOPIC       = "raw-data"
OUTPUT_TOPIC      = "processed-data"
GROUP_ID          = "data-processor-group"


# Препроцессинг (повторяет логику из EDA)

RATING_TO_LABEL = {1: 0, 2: 0, 4: 1, 5: 1}   # 0=negative, 1=positive, 3 дропаем


def clean_text(text: str) -> str:
    text = text.lower().strip()
    text = re.sub(r"<[^>]+>", " ", text)             # убираем HTML-теги
    text = re.sub(r"http\S+|www\S+", "", text)        # убираем URL
    text = re.sub(r"[^a-z0-9\s.,!?']", " ", text)    # оставляем только нужные символы
    text = re.sub(r"\s+", " ", text).strip()
    return text


def preprocess(record: dict) -> dict | None:
    rating = int(record.get("rating", 0))

    # Дропаем нейтральный класс — обосновано в EDA (label noise)
    if rating == 3:
        return None

    label = RATING_TO_LABEL.get(rating)
    if label is None:
        return None

    text       = clean_text(record.get("text", ""))
    title      = clean_text(record.get("title", ""))
    input_text = f"{title} [SEP] {text}".strip()

    # Дропаем слишком короткие тексты — порог из EDA (text_len >= 10)
    if len(input_text) < 10:
        return None

    return {
        "asin":              record.get("asin", ""),
        "parent_asin":       record.get("parent_asin", ""),
        "product_title":     record.get("product_title", ""),
        "rating":            rating,
        # Текстовые поля для модели и для отображения на дашборде
        "title":             record.get("title", ""),
        "text":              record.get("text", ""),
        "label":             label,                   # 0=negative, 1=positive
        "sentiment":         "negative" if label == 0 else "positive",
        "input_text":        input_text,
        "text_len":          len(text),
        "word_count":        len(text.split()),
        "verified_purchase": record.get("verified_purchase", False),
        "timestamp":         record.get("timestamp", ""),
        "producer":          record.get("producer", ""),
    }



def main():
    consumer = BaseConsumer(BOOTSTRAP_SERVERS, INPUT_TOPIC, GROUP_ID)
    producer = BaseProducer(BOOTSTRAP_SERVERS, OUTPUT_TOPIC)

    total_in  = 0
    total_out = 0
    dropped   = 0

    logger.info("DataProcessorConsumer started")

    try:
        for key, record in consumer.consume():
            total_in += 1

            processed = preprocess(record)

            if processed is None:
                dropped += 1
                continue

            producer.send(processed, key=processed["asin"])
            total_out += 1

            if total_in % 500 == 0:
                logger.info(
                    f"in={total_in:,} | out={total_out:,} | "
                    f"dropped={dropped:,} ({dropped / total_in * 100:.1f}%)"
                )

    except KeyboardInterrupt:
        logger.info("Shutting down DataProcessorConsumer")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
