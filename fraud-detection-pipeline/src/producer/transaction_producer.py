import json
import time
import random
import uuid
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import argparse

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config.app_config import Config, COUNTRY_RISK_SCORES, MERCHANT_RISK_SCORES

try:
    from kafka import KafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    print(" kafka-python not installed. Running in file-only mode.")

try:
    from faker import Faker
    fake = Faker()
    Faker.seed(42)
except ImportError:
    fake = None
    print(" Faker not installed. Using basic data generation.")

random.seed(42)

class TransactionProducer:
    
    def __init__(self, bootstrap_servers: str = None, write_to_file: bool = True):
        self.bootstrap_servers = bootstrap_servers or Config.KAFKA_BOOTSTRAP_SERVERS
        self.write_to_file = write_to_file
        self.producer = None
        
        if KAFKA_AVAILABLE:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    key_serializer=lambda k: k.encode('utf-8') if k else None,
                    acks='all',
                    retries=3,
                    max_in_flight_requests_per_connection=1
                )
                print(f" Kafka producer connected to {self.bootstrap_servers}")
            except Exception as e:
                print(f" Kafka connection failed: {e}. File-only mode.")
                self.producer = None
        
        self.users = [f"USER_{i:05d}" for i in range(1000)]
        self.merchants = [f"MERCHANT_{i:03d}" for i in range(100)]
        self.countries = list(COUNTRY_RISK_SCORES.keys())
        self.categories = list(MERCHANT_RISK_SCORES.keys())
        
        self.recent_transactions: List[Dict] = []
        
        self.stats = {
            "total": 0,
            "normal": 0,
            "fraud": 0,
            "late": 0,
            "duplicate": 0
        }
        
        self.data_dir = Config.HDFS_MASTER_DATASET
        os.makedirs(self.data_dir, exist_ok=True)
    
    def generate_normal_transaction(self) -> Dict:
        user_id = random.choice(self.users)
        amount = round(random.lognormvariate(3.5, 1.2), 2)
        amount = min(amount, 2000)
        
        timestamp = datetime.utcnow()
        
        return {
            "transaction_id": str(uuid.uuid4()),
            "user_id": user_id,
            "amount": amount,
            "currency": "USD",
            "merchant_id": random.choice(self.merchants[:80]),
            "merchant_category": random.choice(["grocery", "retail", "restaurant", "utilities"]),
            "country": random.choice(["US", "UK", "CA", "DE", "FR"]),
            "card_type": random.choice(["visa", "mastercard", "amex"]),
            "is_online": random.random() < 0.4,
            "timestamp": timestamp.isoformat() + "Z",
            "event_time": timestamp.isoformat() + "Z",
            "hour_of_day": timestamp.hour,
            "_label": "normal"
        }
    
    def generate_fraudulent_transaction(self) -> Dict:
        user_id = random.choice(self.users)
        amount = round(random.uniform(1000, 10000), 2)
        
        timestamp = datetime.utcnow()
        
        return {
            "transaction_id": str(uuid.uuid4()),
            "user_id": user_id,
            "amount": amount,
            "currency": "USD",
            "merchant_id": random.choice(self.merchants[80:] + ["MERCHANT_999"]),
            "merchant_category": random.choice(["crypto", "gambling", "jewelry", "wire_transfer"]),
            "country": random.choice(["NG", "RU", "UA", "UNKNOWN"]),
            "card_type": random.choice(["visa", "mastercard"]),
            "is_online": random.random() < 0.9,
            "timestamp": timestamp.isoformat() + "Z",
            "event_time": timestamp.isoformat() + "Z",
            "hour_of_day": timestamp.hour,
            "_label": "fraud"
        }
    
    def generate_late_event(self) -> Dict:
        transaction = self.generate_normal_transaction()
        
        delay_seconds = random.randint(60, 300)
        event_time = datetime.utcnow() - timedelta(seconds=delay_seconds)
        
        transaction["event_time"] = event_time.isoformat() + "Z"
        transaction["_label"] = "late"
        transaction["_delay_seconds"] = delay_seconds
        
        return transaction
    
    def generate_duplicate_event(self) -> Optional[Dict]:
        if not self.recent_transactions:
            return self.generate_normal_transaction()
        
        original = random.choice(self.recent_transactions[-10:])
        duplicate = original.copy()
        duplicate["timestamp"] = datetime.utcnow().isoformat() + "Z"
        duplicate["_label"] = "duplicate"
        duplicate["_original_id"] = original["transaction_id"]
        
        return duplicate
    
    def generate_transaction(self) -> Dict:
        rand = random.random()
        
        if rand < Config.PRODUCER_FRAUD_RATE:
            transaction = self.generate_fraudulent_transaction()
            self.stats["fraud"] += 1
        elif rand < Config.PRODUCER_FRAUD_RATE + Config.PRODUCER_LATE_EVENT_RATE:
            transaction = self.generate_late_event()
            self.stats["late"] += 1
        elif rand < Config.PRODUCER_FRAUD_RATE + Config.PRODUCER_LATE_EVENT_RATE + Config.PRODUCER_DUPLICATE_RATE:
            transaction = self.generate_duplicate_event()
            self.stats["duplicate"] += 1
        else:
            transaction = self.generate_normal_transaction()
            self.stats["normal"] += 1
        
        self.stats["total"] += 1
        
        if transaction["_label"] != "duplicate":
            self.recent_transactions.append(transaction)
            if len(self.recent_transactions) > 100:
                self.recent_transactions.pop(0)
        
        return transaction
    
    def send_to_kafka(self, transaction: Dict) -> bool:
        if not self.producer:
            return False
        
        try:
            future = self.producer.send(
                Config.KAFKA_TOPIC_TRANSACTIONS,
                key=transaction["user_id"],
                value=transaction
            )
            future.get(timeout=10)
            return True
        except Exception as e:
            print(f" Kafka send error: {e}")
            return False
    
    def write_to_filesystem(self, transaction: Dict):
        if not self.write_to_file:
            return
        
        event_time = datetime.fromisoformat(transaction["event_time"].replace("Z", ""))
        partition_path = os.path.join(
            self.data_dir,
            f"year={event_time.year}",
            f"month={event_time.month:02d}",
            f"day={event_time.day:02d}"
        )
        os.makedirs(partition_path, exist_ok=True)
        
        filename = os.path.join(partition_path, "transactions.jsonl")
        with open(filename, "a") as f:
            f.write(json.dumps(transaction) + "\n")
    
    def run(self, tps: int = None, duration: int = None, max_transactions: int = None):
        tps = tps or Config.PRODUCER_TPS
        interval = 1.0 / tps
        
        print(f" Starting producer: {tps} TPS")
        print(f"   Kafka: {self.bootstrap_servers}")
        print(f"   File output: {self.data_dir}")
        print(f"   Fraud rate: {Config.PRODUCER_FRAUD_RATE * 100}%")
        print("-" * 50)
        
        start_time = time.time()
        count = 0
        
        try:
            while True:
                if duration and (time.time() - start_time) >= duration:
                    break
                if max_transactions and count >= max_transactions:
                    break
                
                transaction = self.generate_transaction()
                
                kafka_success = self.send_to_kafka(transaction)
                
                self.write_to_filesystem(transaction)
                
                count += 1
                
                if count % 100 == 0:
                    elapsed = time.time() - start_time
                    actual_tps = count / elapsed
                    kafka_status = "" if kafka_success else ""
                    print(f"[{count:6d}] {kafka_status} TPS: {actual_tps:.1f} | "
                          f"Normal: {self.stats['normal']} | "
                          f"Fraud: {self.stats['fraud']} | "
                          f"Late: {self.stats['late']} | "
                          f"Dup: {self.stats['duplicate']}")
                
                time.sleep(interval)
                
        except KeyboardInterrupt:
            print("\n Producer stopped by user")
        finally:
            self.print_summary()
            if self.producer:
                self.producer.flush()
                self.producer.close()
    
    def print_summary(self):
        print("\n" + "=" * 50)
        print("PRODUCTION SUMMARY")
        print("=" * 50)
        print(f"Total transactions: {self.stats['total']}")
        print(f"  Normal:     {self.stats['normal']:6d} ({self.stats['normal']/max(1,self.stats['total'])*100:.1f}%)")
        print(f"  Fraudulent: {self.stats['fraud']:6d} ({self.stats['fraud']/max(1,self.stats['total'])*100:.1f}%)")
        print(f"  Late:       {self.stats['late']:6d} ({self.stats['late']/max(1,self.stats['total'])*100:.1f}%)")
        print(f"  Duplicate:  {self.stats['duplicate']:6d} ({self.stats['duplicate']/max(1,self.stats['total'])*100:.1f}%)")
        print("=" * 50)

def main():
    parser = argparse.ArgumentParser(description="Transaction Producer")
    parser.add_argument("--tps", type=int, default=10, help="Transactions per second")
    parser.add_argument("--duration", type=int, default=None, help="Duration in seconds")
    parser.add_argument("--max", type=int, default=None, help="Max transactions")
    parser.add_argument("--kafka", type=str, default=None, help="Kafka bootstrap servers")
    parser.add_argument("--no-file", action="store_true", help="Disable file output")
    
    args = parser.parse_args()
    
    producer = TransactionProducer(
        bootstrap_servers=args.kafka,
        write_to_file=not args.no_file
    )
    
    producer.run(
        tps=args.tps,
        duration=args.duration,
        max_transactions=args.max
    )

if __name__ == "__main__":
    main()
