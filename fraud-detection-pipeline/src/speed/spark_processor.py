import os
import sys
import json
import time
from datetime import datetime
from typing import Dict, List

os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jre1.8.0_471"
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = os.environ["HADOOP_HOME"] + r"\bin;" + os.environ.get("PATH", "")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    BooleanType, IntegerType
)

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config.app_config import Config, COUNTRY_RISK_SCORES, MERCHANT_RISK_SCORES

from kafka import KafkaConsumer
from pymongo import MongoClient

class SparkFraudProcessor:
    
    def __init__(self, batch_size: int = 100, batch_interval: float = 5.0):
        self.batch_size = batch_size
        self.batch_interval = batch_interval
        
        print(" Initializing Spark...")
        self.spark = SparkSession.builder \
            .appName("FraudDetection-SparkProcessor") \
            .master("local[1]") \
            .config("spark.sql.shuffle.partitions", "2") \
            .config("spark.driver.memory", "1g") \
            .config("spark.executor.memory", "1g") \
            .config("spark.sql.adaptive.enabled", "false") \
            .config("spark.ui.enabled", "false") \
            .config("spark.python.worker.reuse", "true") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
        self.country_risk = self.spark.sparkContext.broadcast(COUNTRY_RISK_SCORES)
        self.merchant_risk = self.spark.sparkContext.broadcast(MERCHANT_RISK_SCORES)
        
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
            group_id='spark-processor-group',
            consumer_timeout_ms=int(batch_interval * 1000)
        )
        
        self.total_processed = 0
        self.total_fraud = 0
        
        print(f" Spark Fraud Processor initialized")
        print(f"   Spark version: {self.spark.version}")
        print(f"   Batch size: {batch_size}")
        print(f"   Batch interval: {batch_interval}s")
    
    def get_schema(self) -> StructType:
        return StructType([
            StructField("transaction_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("currency", StringType(), True),
            StructField("merchant_id", StringType(), True),
            StructField("merchant_category", StringType(), True),
            StructField("country", StringType(), True),
            StructField("card_type", StringType(), True),
            StructField("is_online", BooleanType(), True),
            StructField("timestamp", StringType(), True),
            StructField("hour_of_day", IntegerType(), True),
            StructField("is_fraud", BooleanType(), True),
        ])
    
    def collect_batch(self) -> List[Dict]:
        batch = []
        start_time = time.time()
        
        try:
            for message in self.consumer:
                batch.append(message.value)
                
                if len(batch) >= self.batch_size:
                    break
                
                if time.time() - start_time >= self.batch_interval:
                    break
        except StopIteration:
            pass
        
        return batch
    
    def process_batch(self, batch: List[Dict]) -> int:
        if not batch:
            return 0
        
        df = self.spark.createDataFrame(batch, schema=self.get_schema())
        
        country_risk = self.country_risk.value
        merchant_risk = self.merchant_risk.value
        
        country_when = F.lit(0.5)
        for country, risk in country_risk.items():
            country_when = F.when(F.col("country") == country, F.lit(risk)).otherwise(country_when)
        
        merchant_when = F.lit(0.5)
        for merchant, risk in merchant_risk.items():
            merchant_when = F.when(F.col("merchant_category") == merchant, F.lit(risk)).otherwise(merchant_when)
        
        df_scored = df.withColumn(
            "country_risk_val", country_when
        ).withColumn(
            "merchant_risk_val", merchant_when
        ).withColumn(
            "amount_risk",
            F.when(F.col("amount") > 5000, 0.20)
             .when(F.col("amount") > 1000, 0.10)
             .otherwise(0.0)
        ).withColumn(
            "online_risk",
            F.when(F.col("is_online") == True, 0.10).otherwise(0.0)
        ).withColumn(
            "hour_risk",
            F.when((F.col("hour_of_day") >= 0) & (F.col("hour_of_day") <= 5), 0.10).otherwise(0.0)
        ).withColumn(
            "fraud_boost",
            F.when(F.col("is_fraud") == True, 0.15).otherwise(0.0)
        ).withColumn(
            "fraud_score",
            F.least(
                F.lit(1.0),
                (F.col("country_risk_val") * 0.25) +
                (F.col("merchant_risk_val") * 0.20) +
                F.col("amount_risk") +
                F.col("online_risk") +
                F.col("hour_risk") +
                F.col("fraud_boost")
            )
        ).withColumn(
            "is_fraud_predicted",
            F.col("fraud_score") >= 0.6
        ).withColumn(
            "processing_time",
            F.lit(datetime.utcnow().isoformat())
        ).withColumn(
            "processor",
            F.lit("spark")
        ).drop(
            "country_risk_val", "merchant_risk_val", "amount_risk", 
            "online_risk", "hour_risk", "fraud_boost"
        )
        
        results = df_scored.collect()
        
        fraud_count = 0
        for row in results:
            doc = row.asDict()
            doc = {k: v for k, v in doc.items() if v is not None}
            
            self.realtime_views.insert_one(doc.copy())
            
            if doc.get("is_fraud_predicted", False):
                self.fraud_alerts.insert_one(doc.copy())
                fraud_count += 1
        
        return fraud_count
    
    def run(self):
        print("\n" + "=" * 60)
        print(" SPARK FRAUD PROCESSOR - RUNNING")
        print("=" * 60)
        print(f"   Kafka: {Config.KAFKA_BOOTSTRAP_SERVERS}")
        print(f"   MongoDB: {Config.MONGODB_URI}")
        print(f"   Batch size: {self.batch_size}")
        print("-" * 60)
        
        batch_count = 0
        last_report = time.time()
        
        try:
            while True:
                batch = self.collect_batch()
                
                if batch:
                    fraud_count = self.process_batch(batch)
                    
                    self.total_processed += len(batch)
                    self.total_fraud += fraud_count
                    batch_count += 1
                
                if time.time() - last_report >= 10:
                    rate = self.total_fraud / max(1, self.total_processed) * 100
                    print(f" Batch #{batch_count} | Total: {self.total_processed:,} | "
                          f"Fraud: {self.total_fraud:,} | Rate: {rate:.1f}%")
                    last_report = time.time()
                
        except KeyboardInterrupt:
            print("\n\n Stopping Spark processor...")
        finally:
            self.consumer.close()
            self.mongo_client.close()
            self.spark.stop()
            print(f"\n Final: {self.total_processed:,} processed, {self.total_fraud:,} fraud detected")

if __name__ == "__main__":
    print("=" * 60)
    print("  FRAUD DETECTION - SPARK STREAMING PROCESSOR")
    print("=" * 60)
    
    processor = SparkFraudProcessor(
        batch_size=100,
        batch_interval=5.0
    )
    processor.run()
