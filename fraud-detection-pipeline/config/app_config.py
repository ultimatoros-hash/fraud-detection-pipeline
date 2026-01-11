import os
from dataclasses import dataclass, field
from typing import Dict, List

COUNTRY_RISK_SCORES: Dict[str, float] = {
    "US": 0.1,
    "UK": 0.1,
    "CA": 0.1,
    "DE": 0.15,
    "FR": 0.15,
    "JP": 0.1,
    "AU": 0.1,
    "NZ": 0.1,
    "CH": 0.1,
    "SE": 0.1,
    "BR": 0.4,
    "MX": 0.4,
    "IN": 0.35,
    "CN": 0.4,
    "RU": 0.5,
    "TR": 0.45,
    "ZA": 0.4,
    "AE": 0.35,
    "NG": 0.8,
    "UA": 0.7,
    "PK": 0.65,
    "VN": 0.6,
    "PH": 0.55,
    "ID": 0.5,
    "KE": 0.65,
    "GH": 0.7,
    "UNKNOWN": 0.9,
}

MERCHANT_RISK_SCORES: Dict[str, float] = {
    "grocery": 0.1,
    "retail": 0.15,
    "restaurant": 0.15,
    "utilities": 0.1,
    "healthcare": 0.1,
    "education": 0.1,
    "electronics": 0.35,
    "travel": 0.4,
    "hotel": 0.35,
    "entertainment": 0.3,
    "subscription": 0.25,
    "gambling": 0.85,
    "crypto": 0.9,
    "jewelry": 0.7,
    "luxury": 0.65,
    "adult": 0.8,
    "wire_transfer": 0.75,
    "unknown": 0.5,
}

BLACKLISTED_MERCHANTS: List[str] = [
    "MERCHANT_999",
    "MERCHANT_998",
    "MERCHANT_997",
]

@dataclass
class Config:
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_TOPIC_TRANSACTIONS: str = os.getenv("KAFKA_TOPIC_TRANSACTIONS", "transactions")
    KAFKA_TOPIC_FRAUD_ALERTS: str = os.getenv("KAFKA_TOPIC_FRAUD_ALERTS", "fraud-alerts")
    KAFKA_CONSUMER_GROUP: str = os.getenv("KAFKA_CONSUMER_GROUP", "fraud-detection-group")
    KAFKA_NUM_PARTITIONS: int = int(os.getenv("KAFKA_NUM_PARTITIONS", "6"))
    KAFKA_REPLICATION_FACTOR: int = int(os.getenv("KAFKA_REPLICATION_FACTOR", "1"))
    SPARK_APP_NAME_STREAM: str = "FraudDetection-SpeedLayer"
    SPARK_MASTER: str = os.getenv("SPARK_MASTER", "local[*]")
    SPARK_TRIGGER_INTERVAL: str = os.getenv("SPARK_TRIGGER_INTERVAL", "5 seconds")
    SPARK_WATERMARK_DELAY: str = os.getenv("SPARK_WATERMARK_DELAY", "30 seconds")
    SPARK_WINDOW_DURATION: str = os.getenv("SPARK_WINDOW_DURATION", "1 minute")
    SPARK_WINDOW_SLIDE: str = os.getenv("SPARK_WINDOW_SLIDE", "30 seconds")
    SPARK_CHECKPOINT_STREAM: str = os.getenv(
        "SPARK_CHECKPOINT_STREAM", 
        "./checkpoints/speed-layer"
    )
    SPARK_STATE_STORE: str = os.getenv("SPARK_STATE_STORE", "rocksdb")
    SPARK_SHUFFLE_PARTITIONS: int = int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "6"))
    SPARK_APP_NAME_BATCH: str = "FraudDetection-BatchLayer"
    BATCH_INTERVAL_HOURS: int = int(os.getenv("BATCH_INTERVAL_HOURS", "1"))
    SPARK_CHECKPOINT_BATCH: str = os.getenv(
        "SPARK_CHECKPOINT_BATCH",
        "./checkpoints/batch-layer"
    )
    SPARK_BATCH_PARTITIONS: int = int(os.getenv("SPARK_BATCH_PARTITIONS", "12"))
    HDFS_MASTER_DATASET: str = os.getenv("HDFS_MASTER_DATASET", "./data/raw/transactions")
    HDFS_BATCH_OUTPUT: str = os.getenv("HDFS_BATCH_OUTPUT", "./data/master/batch_views")
    HDFS_PARTITION_COLS: List[str] = field(default_factory=lambda: ["year", "month", "day"])
    HDFS_DATA_FORMAT: str = os.getenv("HDFS_DATA_FORMAT", "parquet")
    MONGODB_URI: str = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
    MONGODB_DATABASE: str = os.getenv("MONGODB_DATABASE", "fraud_detection")
    MONGODB_COLLECTION_BATCH_VIEWS: str = "batch_views"
    MONGODB_COLLECTION_REALTIME_VIEWS: str = "realtime_views"
    MONGODB_COLLECTION_FRAUD_ALERTS: str = "fraud_alerts"
    MONGODB_COLLECTION_USER_PROFILES: str = "user_profiles"
    MONGODB_COLLECTION_ML_MODELS: str = "ml_models"
    MONGODB_REALTIME_TTL: int = int(os.getenv("MONGODB_REALTIME_TTL", "3600"))
    FRAUD_THRESHOLD: float = float(os.getenv("FRAUD_THRESHOLD", "0.7"))
    VELOCITY_WINDOW_MINUTES: int = int(os.getenv("VELOCITY_WINDOW_MINUTES", "5"))
    VELOCITY_COUNT_THRESHOLD: int = int(os.getenv("VELOCITY_COUNT_THRESHOLD", "5"))
    VELOCITY_AMOUNT_THRESHOLD: float = float(os.getenv("VELOCITY_AMOUNT_THRESHOLD", "5000.0"))
    HIGH_AMOUNT_THRESHOLD: float = float(os.getenv("HIGH_AMOUNT_THRESHOLD", "1000.0"))
    VERY_HIGH_AMOUNT_THRESHOLD: float = float(os.getenv("VERY_HIGH_AMOUNT_THRESHOLD", "5000.0"))
    DEDUP_WINDOW_SECONDS: int = int(os.getenv("DEDUP_WINDOW_SECONDS", "300"))
    ML_MODEL_PATH: str = os.getenv("ML_MODEL_PATH", "./models/fraud_model")
    ML_FEATURES: List[str] = field(default_factory=lambda: [
        "amount_normalized",
        "country_risk",
        "merchant_risk",
        "is_online",
        "hour_of_day",
        "velocity_count",
        "velocity_amount",
    ])
    ML_TRAIN_TEST_SPLIT: float = 0.8
    ML_MAX_ITER: int = 100
    ML_REG_PARAM: float = 0.01
    PRODUCER_TPS: int = int(os.getenv("PRODUCER_TPS", "10"))
    PRODUCER_FRAUD_RATE: float = float(os.getenv("PRODUCER_FRAUD_RATE", "0.05"))
    PRODUCER_LATE_EVENT_RATE: float = float(os.getenv("PRODUCER_LATE_EVENT_RATE", "0.02"))
    PRODUCER_DUPLICATE_RATE: float = float(os.getenv("PRODUCER_DUPLICATE_RATE", "0.01"))
    METRICS_PORT: int = int(os.getenv("METRICS_PORT", "8080"))
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")

TRANSACTION_SCHEMA = {
    "type": "struct",
    "fields": [
        {"name": "transaction_id", "type": "string", "nullable": False},
        {"name": "user_id", "type": "string", "nullable": False},
        {"name": "amount", "type": "double", "nullable": False},
        {"name": "currency", "type": "string", "nullable": True},
        {"name": "merchant_id", "type": "string", "nullable": False},
        {"name": "merchant_category", "type": "string", "nullable": True},
        {"name": "country", "type": "string", "nullable": True},
        {"name": "card_type", "type": "string", "nullable": True},
        {"name": "is_online", "type": "boolean", "nullable": True},
        {"name": "timestamp", "type": "string", "nullable": False},
        {"name": "event_time", "type": "string", "nullable": False},
    ]
}

def get_kafka_options() -> Dict[str, str]:
    return {
        "kafka.bootstrap.servers": Config.KAFKA_BOOTSTRAP_SERVERS,
        "subscribe": Config.KAFKA_TOPIC_TRANSACTIONS,
        "startingOffsets": "latest",
        "failOnDataLoss": "false",
        "kafka.consumer.commit.groupid": Config.KAFKA_CONSUMER_GROUP,
    }

def get_mongodb_options(collection: str) -> Dict[str, str]:
    return {
        "spark.mongodb.output.uri": Config.MONGODB_URI,
        "spark.mongodb.output.database": Config.MONGODB_DATABASE,
        "spark.mongodb.output.collection": collection,
    }

def print_config():
    print("=" * 60)
    print("FRAUD DETECTION PIPELINE CONFIGURATION (LAMBDA ARCHITECTURE)")
    print("=" * 60)
    print(f"Kafka Bootstrap: {Config.KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Kafka Topic: {Config.KAFKA_TOPIC_TRANSACTIONS}")
    print(f"MongoDB URI: {Config.MONGODB_URI}")
    print(f"MongoDB Database: {Config.MONGODB_DATABASE}")
    print(f"HDFS Master Dataset: {Config.HDFS_MASTER_DATASET}")
    print(f"HDFS Batch Output: {Config.HDFS_BATCH_OUTPUT}")
    print(f"Fraud Threshold: {Config.FRAUD_THRESHOLD}")
    print(f"Speed Layer Checkpoint: {Config.SPARK_CHECKPOINT_STREAM}")
    print(f"Batch Layer Checkpoint: {Config.SPARK_CHECKPOINT_BATCH}")
    print(f"Batch Interval: {Config.BATCH_INTERVAL_HOURS} hours")
    print("=" * 60)

if __name__ == "__main__":
    print_config()
