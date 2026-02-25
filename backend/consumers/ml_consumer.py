import os
import sys
import logging
import re
from datetime import datetime, timezone
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification

sys.path.append("/app/producers")

from base_consumer import BaseConsumer
from base_producer import BaseProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s"
)
logger = logging.getLogger("MLConsumer")

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-broker-1:9092")
INPUT_TOPIC       = "processed-data"   # читает с Broker 2
OUTPUT_TOPIC      = "ml-results"       # пишет на Broker 1 (feedback loop)
GROUP_ID          = "ml-consumer-group"
MODEL_PATH        = os.getenv("MODEL_PATH", "/app/model/sentiment_distilbert")
MODEL_NAME        = "distilbert-base-uncased"
MAX_LENGTH        = 160                # из EDA: P95 = 144, с буфером = 160
ALERT_THRESHOLD   = 0.85              # алерт если негатив с уверенностью > 85%
DEVICE            = "cuda" if torch.cuda.is_available() else "cpu"

LABEL_MAP = {0: "negative", 1: "positive"}


# Rule-based аспекты для простого извлечения причин негатива (по ключевым словам в тексте отзыва)

ASPECT_PATTERNS: dict[str, list[re.Pattern]] = {
    # Логистика
    "delivery": [
        re.compile(r"\b(delivery|shipping|shipped|arrive[ds]?|late|on time|courier)\b", re.IGNORECASE),
    ],
    # Упаковка
    "packaging": [
        re.compile(r"\b(packaging|package|box|bottle|tube|leak(?:ed|ing)?|broken|damaged)\b", re.IGNORECASE),
    ],
    # Запах / аромат
    "smell": [
        re.compile(r"\b(smell|odor|odour|fragrance|scent|stink|stinky)\b", re.IGNORECASE),
    ],
    # Качество / эффект
    "quality": [
        re.compile(r"\b(quality|cheap(?:ly)?|poor|bad|good quality|effective|does not work|didn't work|ineffective)\b", re.IGNORECASE),
    ],
    # Цена / ценность
    "price": [
        re.compile(r"\b(price|expensive|overpriced|cheap|value for money|worth the money)\b", re.IGNORECASE),
    ],
    # Цвет / оттенок
    "color": [
        re.compile(r"\b(color|colour|shade|too dark|too light|match(?:es|ed)? my skin)\b", re.IGNORECASE),
    ],
    # Состав / кожа / аллергия
    "ingredients": [
        re.compile(r"\b(ingredient[s]?|paraben[s]?|sulfate[s]?|alcohol free|natural)\b", re.IGNORECASE),
    ],
    "skin_reaction": [
        re.compile(r"\b(irritat(?:ed|ion)|rash|breakout[s]?|acne|allergic?|burn(?:ed|ing)?)\b", re.IGNORECASE),
    ],
    # Объем / длительность
    "size_durability": [
        re.compile(r"\b(size|tiny|small|bottle is (?:too )?small|last(?:s|ed)? long|doesn't last|run[s]? out fast)\b", re.IGNORECASE),
    ],
}


def extract_aspects(text: str) -> list[str]:
    """
    Простое rule-based извлечение аспектов на основе ключевых слов.
    Возвращает список уникальных аспектов, упомянутых в отзыве.
    """
    if not text:
        return []

    aspects: list[str] = []
    for aspect, patterns in ASPECT_PATTERNS.items():
        if any(p.search(text) for p in patterns):
            aspects.append(aspect)

    # Сортируем для стабильности и убираем дубликаты
    return sorted(set(aspects))


def load_model():
    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
    model = AutoModelForSequenceClassification.from_pretrained(
        MODEL_NAME, num_labels=2
    )

    if os.path.exists(MODEL_PATH):
        # Загружаем веса дообученного DistilBERT
        model = AutoModelForSequenceClassification.from_pretrained(MODEL_PATH)
        logger.info(f"Loaded fine-tuned model from {MODEL_PATH}")
    else:
        logger.warning(f"Fine-tuned weights not found at {MODEL_PATH}, using base model!")

    model.to(DEVICE)
    model.eval()
    logger.info(f"Model ready on device={DEVICE}")
    return tokenizer, model



@torch.no_grad()
def predict(input_text: str, tokenizer, model) -> dict:
    enc = tokenizer(
        input_text,
        max_length=MAX_LENGTH,
        padding="max_length",
        truncation=True,
        return_tensors="pt",
    )
    input_ids = enc["input_ids"].to(DEVICE)
    attn_mask = enc["attention_mask"].to(DEVICE)

    logits = model(input_ids=input_ids, attention_mask=attn_mask).logits
    probs  = torch.softmax(logits, dim=-1).cpu().numpy()[0]
    pred   = int(probs.argmax())

    return {
        "predicted_label": LABEL_MAP[pred],
        "prob_negative":   round(float(probs[0]), 4),
        "prob_positive":   round(float(probs[1]), 4),
        "confidence":      round(float(probs.max()), 4),
    }



def main():
    tokenizer, model = load_model()

    consumer = BaseConsumer(BOOTSTRAP_SERVERS, INPUT_TOPIC, GROUP_ID)
    producer = BaseProducer(BOOTSTRAP_SERVERS, OUTPUT_TOPIC)

    total      = 0
    alerts     = 0
    correct    = 0   # для подсчета accuracy на лету

    logger.info("MLConsumer started.")

    try:
        for key, record in consumer.consume():
            input_text = record.get("input_text", "")
            if not input_text:
                continue

            result = predict(input_text, tokenizer, model)

            # Rule-based аспекты (по заголовку + полному тексту отзыва)
            full_text = f"{record.get('title', '')} [SEP] {record.get('text', '')}"
            aspect_tags = extract_aspects(full_text)

            # Проверяем совпадение с реальным лейблом (он есть из DataProcessor)
            true_label = record.get("sentiment", "")
            is_correct = (true_label == result["predicted_label"])
            if is_correct:
                correct += 1

            # Алерт: модель уверена что отзыв негативный
            is_alert = (
                result["predicted_label"] == "negative"
                and result["confidence"] >= ALERT_THRESHOLD
            )
            if is_alert:
                alerts += 1

            # Если timestamp из пайплайна пустой, используем время обработки
            ts = record.get("timestamp", "")
            if not ts:
                ts = datetime.now(timezone.utc).isoformat()

            total += 1

            output = {
                # Данные о товаре
                "asin":              record.get("asin", ""),
                "product_title":     record.get("product_title", ""),
                "rating":            record.get("rating"),
                # Текст отзыва для отображения
                "title":             record.get("title", ""),
                "text":              record.get("text", ""),
                # Реальная и предсказанная метки
                "true_sentiment":    true_label,
                "predicted_label":   result["predicted_label"],
                # Вероятности
                "prob_negative":     result["prob_negative"],
                "prob_positive":     result["prob_positive"],
                "confidence":        result["confidence"],
                # Аспектный анализ (rule-based)
                "aspect_tags":       aspect_tags,
                # Мета
                "is_alert":          is_alert,
                "timestamp":         ts,
                "producer":          record.get("producer", ""),
                # Счётчики для дашборда (кумулятивные в рамках жизни консьюмера)
                "total_processed":   total,
                "total_alerts":      alerts,
            }

            # Пишем в ml-results → идет на Broker 1 (feedback loop)
            producer.send(output, key=output["asin"])

            if total % 100 == 0:
                acc = correct / total * 100
                logger.info(
                    f"processed={total:,} | "
                    f"alerts={alerts:,} | "
                    f"online_acc={acc:.1f}%"
                )

    except KeyboardInterrupt:
        logger.info("Shutting down MLConsumer...")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
