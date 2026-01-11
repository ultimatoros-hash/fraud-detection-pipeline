#!/usr/bin/env python3

import os
import sys
import subprocess
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent

def check_python_version():
    if sys.version_info < (3, 9):
        print(" Python 3.9+ is required")
        return False
    print(f" Python {sys.version_info.major}.{sys.version_info.minor}")
    return True

def check_java():
    try:
        result = subprocess.run(
            ["java", "-version"],
            capture_output=True,
            text=True
        )
        version_line = result.stderr.split('\n')[0]
        print(f" Java: {version_line}")
        return True
    except FileNotFoundError:
        print(" Java not found. Install Java 11+")
        return False

def install_dependencies():
    print("\n Installing Python dependencies...")
    
    req_file = PROJECT_ROOT / "requirements.txt"
    
    result = subprocess.run(
        [sys.executable, "-m", "pip", "install", "-r", str(req_file)],
        capture_output=True,
        text=True
    )
    
    if result.returncode == 0:
        print(" Dependencies installed")
        return True
    else:
        print(f" Failed to install dependencies: {result.stderr}")
        return False

def create_directories():
    print("\n Creating directories...")
    
    dirs = [
        "data/raw/transactions",
        "data/processed/batch_views",
        "data/checkpoints/streaming",
        "data/models",
        "logs"
    ]
    
    for d in dirs:
        path = PROJECT_ROOT / d
        path.mkdir(parents=True, exist_ok=True)
        print(f"   {d}")
    
    return True

def check_spark():
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
        version = spark.version
        spark.stop()
        print(f" PySpark {version}")
        return True
    except Exception as e:
        print(f" PySpark issue: {e}")
        return False

def check_mongodb():
    try:
        from pymongo import MongoClient
        sys.path.insert(0, str(PROJECT_ROOT))
        from config.app_config import Config
        
        client = MongoClient(Config.MONGODB_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        print(f" MongoDB connected at {Config.MONGODB_URI}")
        client.close()
        return True
    except Exception as e:
        print(f" MongoDB not available: {e}")
        print("   Start MongoDB or use Docker: docker run -d -p 27017:27017 mongo")
        return False

def check_kafka():
    try:
        from kafka import KafkaProducer
        sys.path.insert(0, str(PROJECT_ROOT))
        from config.app_config import Config
        
        producer = KafkaProducer(
            bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
            api_version_auto_timeout_ms=5000
        )
        producer.close()
        print(f" Kafka connected at {Config.KAFKA_BOOTSTRAP_SERVERS}")
        return True
    except Exception as e:
        print(f" Kafka not available: {e}")
        print("   Start Kafka or use Docker Compose")
        return False

def start_local_services():
    print("\n Starting local services with Docker...")
    
    compose_content = """
version: '3.8'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.4.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:7.4.0
    depends_on:
      - zookeeper
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    ports:
      - "9092:9092"

  mongodb:
    image: mongo:7.0
    ports:
      - "27017:27017"
    volumes:
      - mongodb_data:/data/db

volumes:
  mongodb_data:Run test suite."""
    print("\n Running tests...")
    
    result = subprocess.run(
        [sys.executable, "-m", "pytest", 
         str(PROJECT_ROOT / "tests"), "-v", "--tb=short"],
        cwd=str(PROJECT_ROOT)
    )
    
    return result.returncode == 0

def print_status(checks):
    print("\n" + "=" * 60)
    print("SETUP STATUS")
    print("=" * 60)
    
    all_passed = True
    for name, status in checks.items():
        icon = "" if status else ""
        print(f"{icon} {name}")
        if not status:
            all_passed = False
    
    print("=" * 60)
    
    if all_passed:
        print("\n All checks passed! You're ready to run the pipeline.")
        print("\nNext steps:")
        print("  1. Run producer:  python src/producer/transaction_producer.py")
        print("  2. Run streaming: python src/speed/fraud_detection_stream.py")
        print("  3. Run batch:     python src/batch/fraud_detection_batch.py")
        print("  4. Run dashboard: python src/dashboard/app.py")
        print("\n  Or run all: python scripts/run_pipeline.py")
    else:
        print("\n Some checks failed. Please resolve issues above.")

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Local development setup")
    parser.add_argument("--start-services", action="store_true",
                       help="Start Kafka and MongoDB using Docker")
    args = parser.parse_args()
    
    print("=" * 60)
    print("FRAUD DETECTION PIPELINE - LOCAL SETUP")
    print("=" * 60)
    
    checks = {}
    
    checks["Python Version"] = check_python_version()
    checks["Java Installation"] = check_java()
    checks["Directory Structure"] = create_directories()
    checks["Python Dependencies"] = install_dependencies()
    checks["PySpark"] = check_spark()
    
    if args.start_services:
        start_local_services()
    
    checks["MongoDB"] = check_mongodb()
    checks["Kafka"] = check_kafka()
    
    print_status(checks)

if __name__ == "__main__":
    main()
