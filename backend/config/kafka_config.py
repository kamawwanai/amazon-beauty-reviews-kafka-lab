BOOTSTRAP_SERVERS = "kafka-broker-1:9092,kafka-broker-2:9092"

TOPIC_CONFIGS = {
    # Сырые данные от продюсеров  
    "raw-data": {
        "num_partitions": 2,
        "replication_factor": 2,
        "configs": {
            "min.insync.replicas": "2",          # оба брокера должны подтвердить
            "retention.ms": str(7 * 24 * 3600 * 1000),  # 7 дней
        },
    },
    # Препроцессированные данные  
    "processed-data": {
        "num_partitions": 2,
        "replication_factor": 2,
        "configs": {
            "min.insync.replicas": "2",
            "retention.ms": str(3 * 24 * 3600 * 1000),  # 3 дня
        },
    },
    # Статистика для визуализации
    "visualization-data": {
        "num_partitions": 2,
        "replication_factor": 2,
        "configs": {
            "min.insync.replicas": "1",          # достаточно одного брокера
            "retention.ms": str(1 * 24 * 3600 * 1000),  # 1 день
        },
    },
    # ML-предсказания
    "ml-results": {
        "num_partitions": 2,
        "replication_factor": 2,
        "configs": {
            "min.insync.replicas": "2",
            "retention.ms": str(7 * 24 * 3600 * 1000),  # 7 дней
        },
    },
}
