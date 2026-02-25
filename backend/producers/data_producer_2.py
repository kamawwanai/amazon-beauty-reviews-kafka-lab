import os
import time
import random
import logging
import pandas as pd
from base_producer import BaseProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s"
)
logger = logging.getLogger("DataProducer2")

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-broker-1:9092")
PARQUET_PATH      = os.getenv("PARQUET_PATH", "/app/data/all_beauty_raw.parquet")
TOPIC             = "raw-data"
PRODUCER_ID       = "producer-2"


def load_data(path: str) -> pd.DataFrame:
    df = pd.read_parquet(path)
    # Producer 2 берет вторую половину датасета
    df = df.iloc[len(df) // 2 :].reset_index(drop=True)
    logger.info(f"[{PRODUCER_ID}] Loaded {len(df):,} rows")
    return df


def row_to_record(row, producer_id: str) -> dict:
    return {
        "producer":          producer_id,
        "rating":            int(row["rating"]),
        "text":              str(row.get("text", "")),
        "title":             str(row.get("title", "")),
        "product_title":     str(row.get("product_title", "")),
        "asin":              str(row.get("asin", "")),
        "parent_asin":       str(row.get("parent_asin", "")),
        "timestamp":         str(row.get("dt", "")),
        "verified_purchase": bool(row.get("verified_purchase", False)),
        "helpful_vote":      int(row.get("helpful_vote", 0)),
    }


def main():
    producer = BaseProducer(BOOTSTRAP_SERVERS, TOPIC)
    df = load_data(PARQUET_PATH)

    try:
        while True:
            for _, row in df.iterrows():
                record = row_to_record(row, PRODUCER_ID)
                producer.send(record, key=record["asin"])

                # Producer 2 чуть медленнее
                time.sleep(random.uniform(0.05, 0.15))

            logger.info(f"[{PRODUCER_ID}] Full pass done, restarting")

    except KeyboardInterrupt:
        logger.info(f"[{PRODUCER_ID}] Shutting down")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
