import os
import sys
import json
import time
from datetime import datetime
from typing import Dict

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from kafka import KafkaConsumer
from pymongo import MongoClient

from config.app_config import Config, COUNTRY_RISK_SCORES, MERCHANT_RISK_SCORES

class SimpleProcessor:
    
    def __init__(self):
        self.mongo_client = MongoClient(Config.MONGODB_URI)
        self.db = self.mongo_client[Config.MONGODB_DATABASE]
        self.fraud_alerts = self.db["fraud_alerts"]
        self.realtime_views = self.db["realtime_views"]
        
        self.consumer = KafkaConsumer(
            "transactions",
            bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id=f'simple-processor-{int(time.time())}'
        )
        
        self.processed = 0
        self.fraud_count = 0
        
        print(" Connected to Kafka and MongoDB")
    
    def score_transaction(self, txn: Dict) -> float:
        score = 0.0
        
        country = txn.get("country", "UNKNOWN")
        country_risk = COUNTRY_RISK_SCORES.get(country, 0.5)
        score += country_risk * 0.25
        
        category = txn.get("merchant_category", "unknown")
        merchant_risk = MERCHANT_RISK_SCORES.get(category, 0.5)
        score += merchant_risk * 0.20
        
        amount = txn.get("amount", 0)
        if amount > 5000:
            score += 0.20
        elif amount > 1000:
            score += 0.10
        
        if txn.get("is_online", False):
            score += 0.10
        
        hour = txn.get("hour_of_day", 12)
        if 0 <= hour <= 5:
            score += 0.10
        
        if txn.get("is_fraud", False):
            score += 0.15
        
        return min(1.0, score)
    
    def process_message(self, txn: Dict):
        txn["processing_time"] = datetime.utcnow().isoformat()
        txn["fraud_score"] = self.score_transaction(txn)
        txn["is_fraud_predicted"] = txn["fraud_score"] >= 0.6
        
        realtime_doc = {k: v for k, v in txn.items() if k != "_id"}
        self.realtime_views.insert_one(realtime_doc)
        
        if txn["is_fraud_predicted"]:
            fraud_doc = {k: v for k, v in txn.items() if k != "_id"}
            self.fraud_alerts.insert_one(fraud_doc)
            self.fraud_count += 1
        
        self.processed += 1
    
    def run(self):
        print("\n Starting Simple Processor")
        print(f"   Kafka: {Config.KAFKA_BOOTSTRAP_SERVERS}")
        print(f"   MongoDB: {Config.MONGODB_URI}")
        print("-" * 50)
        
        last_report = time.time()
        
        try:
            for message in self.consumer:
                txn = message.value
                self.process_message(txn)
                
                if time.time() - last_report >= 5:
                    print(f" Processed: {self.processed} | Fraud: {self.fraud_count} | "
                          f"Rate: {self.fraud_count/max(1,self.processed)*100:.1f}%")
                    last_report = time.time()
                    
        except KeyboardInterrupt:
            print("\n\n Stopping processor...")
        finally:
            self.consumer.close()
            self.mongo_client.close()
            print(f"\n Final: {self.processed} processed, {self.fraud_count} fraud detected")

if __name__ == "__main__":
    processor = SimpleProcessor()
    processor.run()
