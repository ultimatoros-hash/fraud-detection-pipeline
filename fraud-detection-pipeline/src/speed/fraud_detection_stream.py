import os
import sys
from datetime import datetime
from typing import Dict

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    BooleanType, TimestampType, IntegerType
)

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config.app_config import Config, COUNTRY_RISK_SCORES, MERCHANT_RISK_SCORES

class FraudDetectionStream:
    
    def __init__(self):
        self.spark = self._create_spark_session()
        
        self.country_risk_bc = self.spark.sparkContext.broadcast(COUNTRY_RISK_SCORES)
        self.merchant_risk_bc = self.spark.sparkContext.broadcast(MERCHANT_RISK_SCORES)
        
        print(f" Speed Layer initialized")
        print(f"   Kafka: {Config.KAFKA_BOOTSTRAP_SERVERS}")
        print(f"   Topic: {Config.KAFKA_TOPIC_TRANSACTIONS}")
        print(f"   Checkpoint: {Config.SPARK_CHECKPOINT_STREAM}")
    
    def _create_spark_session(self) -> SparkSession:
        return SparkSession.builder \
            .appName(Config.SPARK_APP_NAME_STREAM) \
            .master(Config.SPARK_MASTER) \
            .config("spark.sql.shuffle.partitions", Config.SPARK_SHUFFLE_PARTITIONS) \
            .config("spark.streaming.backpressure.enabled", "true") \
            .config("spark.sql.streaming.checkpointLocation", Config.SPARK_CHECKPOINT_STREAM) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()
    
    def get_transaction_schema(self) -> StructType:
        return StructType([
            StructField("transaction_id", StringType(), False),
            StructField("user_id", StringType(), False),
            StructField("amount", DoubleType(), False),
            StructField("currency", StringType(), True),
            StructField("merchant_id", StringType(), False),
            StructField("merchant_category", StringType(), True),
            StructField("country", StringType(), True),
            StructField("card_type", StringType(), True),
            StructField("is_online", BooleanType(), True),
            StructField("timestamp", StringType(), False),
            StructField("event_time", StringType(), False),
            StructField("hour_of_day", IntegerType(), True),
            StructField("_label", StringType(), True),
        ])
    
    def read_kafka_stream(self) -> DataFrame:
        print(" Connecting to Kafka stream...")
        
        return self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", Config.KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", Config.KAFKA_TOPIC_TRANSACTIONS) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .option("maxOffsetsPerTrigger", 1000) \
            .load()
    
    def parse_json(self, df: DataFrame) -> DataFrame:
        print(" Parsing JSON with explicit schema...")
        
        schema = self.get_transaction_schema()
        
        return df \
            .selectExpr("CAST(value AS STRING) as json_value") \
            .select(F.from_json("json_value", schema).alias("data")) \
            .select("data.*") \
            .withColumn("event_timestamp", F.to_timestamp("event_time")) \
            .withColumn("processing_time", F.current_timestamp())
    
    def apply_watermark(self, df: DataFrame) -> DataFrame:
        print(f" Applying watermark: {Config.SPARK_WATERMARK_DELAY}")
        
        return df.withWatermark("event_timestamp", Config.SPARK_WATERMARK_DELAY)
    
    def deduplicate(self, df: DataFrame) -> DataFrame:
        print(" Applying stateful deduplication...")
        
        return df.dropDuplicates(["transaction_id", "event_timestamp"])
    
    def add_risk_scores_udf(self, df: DataFrame) -> DataFrame:
        print(" Adding risk scores via UDFs...")
        
        @F.udf(DoubleType())
        def get_country_risk(country):
            risk_dict = COUNTRY_RISK_SCORES
            return float(risk_dict.get(country, 0.9))
        
        @F.udf(DoubleType())
        def get_merchant_risk(category):
            risk_dict = MERCHANT_RISK_SCORES
            return float(risk_dict.get(category, 0.5))
        
        return df \
            .withColumn("country_risk", get_country_risk(F.col("country"))) \
            .withColumn("merchant_risk", get_merchant_risk(F.col("merchant_category")))
    
    def compute_windowed_aggregates(self, df: DataFrame) -> DataFrame:
        print(f" Computing windowed aggregates: {Config.SPARK_WINDOW_DURATION}")
        
        return df \
            .withColumn("window",
                F.window(
                    "event_timestamp",
                    Config.SPARK_WINDOW_DURATION
                )) \
            .withColumn("window_start", F.col("window.start")) \
            .withColumn("window_end", F.col("window.end")) \
            .drop("window")
    
    def compute_velocity_approximate(self, df: DataFrame) -> DataFrame:
        print(" Computing approximate velocity...")
        
        return df \
            .withColumn("velocity_flag",
                F.when(F.col("amount") > Config.HIGH_AMOUNT_THRESHOLD, 1.0)
                 .otherwise(0.0))
    
    def compute_fraud_score(self, df: DataFrame) -> DataFrame:
        print(" Computing fraud scores...")
        
        @F.udf(DoubleType())
        def calculate_fraud_score(amount, country_risk, merchant_risk, is_online, hour_of_day, velocity_flag):
            score = 0.0
            
            score += (country_risk or 0.5) * 0.25
            
            score += (merchant_risk or 0.5) * 0.20
            
            if amount and amount > 5000:
                score += 0.20
            elif amount and amount > 1000:
                score += 0.10
            
            if is_online:
                score += 0.10
            
            if hour_of_day is not None and 0 <= hour_of_day <= 5:
                score += 0.10
            
            score += (velocity_flag or 0) * 0.15
            
            return min(1.0, max(0.0, score))
        
        df = df.withColumn("fraud_score",
            calculate_fraud_score(
                F.col("amount"),
                F.col("country_risk"),
                F.col("merchant_risk"),
                F.col("is_online"),
                F.col("hour_of_day"),
                F.col("velocity_flag")
            )
        )
        
        df = df.withColumn("is_fraud_predicted",
            F.col("fraud_score") >= Config.FRAUD_THRESHOLD
        )
        
        return df
    
    def write_to_console(self, df: DataFrame):
        print(" Writing to console sink...")
        
        return df.select(
            "transaction_id", "user_id", "amount", "country",
            "fraud_score", "is_fraud_predicted", "processing_time"
        ).writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", False) \
            .trigger(processingTime=Config.SPARK_TRIGGER_INTERVAL) \
            .start()
    
    def write_to_mongodb(self, df: DataFrame, batch_df, batch_id):
        if batch_df.count() == 0:
            return
        
        try:
            from pymongo import MongoClient
            
            alerts = batch_df.filter(F.col("is_fraud_predicted") == True) \
                .select(
                    "transaction_id", "user_id", "amount", "country",
                    "merchant_category", "fraud_score", "is_fraud_predicted",
                    "processing_time"
                ).toPandas()
            
            if len(alerts) > 0:
                client = MongoClient(Config.MONGODB_URI)
                db = client[Config.MONGODB_DATABASE]
                
                alerts["processing_time"] = alerts["processing_time"].astype(str)
                
                db[Config.MONGODB_COLLECTION_REALTIME_VIEWS].insert_many(
                    alerts.to_dict("records")
                )
                
                db[Config.MONGODB_COLLECTION_FRAUD_ALERTS].insert_many(
                    alerts.to_dict("records")
                )
                
                client.close()
                print(f" Batch {batch_id}: Wrote {len(alerts)} alerts to MongoDB")
                
        except Exception as e:
            print(f" MongoDB write error (batch {batch_id}): {e}")
    
    def write_aggregates_to_mongodb(self, df: DataFrame, batch_df, batch_id):
        if batch_df.count() == 0:
            return
        
        try:
            from pymongo import MongoClient
            
            agg_df = batch_df.groupBy("country").agg(
                F.count("*").alias("transaction_count"),
                F.sum("amount").alias("total_amount"),
                F.sum(F.when(F.col("is_fraud_predicted"), 1).otherwise(0)).alias("fraud_count"),
                F.avg("fraud_score").alias("avg_fraud_score")
            ).toPandas()
            
            if len(agg_df) > 0:
                client = MongoClient(Config.MONGODB_URI)
                db = client[Config.MONGODB_DATABASE]
                
                agg_df["timestamp"] = datetime.utcnow().isoformat()
                agg_df["batch_id"] = int(batch_id)
                
                db["dashboard_stats"].insert_many(
                    agg_df.to_dict("records")
                )
                
                client.close()
                
        except Exception as e:
            print(f" Aggregate write error: {e}")
    
    def run(self):
        print("\n" + "=" * 60)
        print("SPEED LAYER - REAL-TIME FRAUD DETECTION")
        print("=" * 60 + "\n")
        
        try:
            raw_stream = self.read_kafka_stream()
            
            parsed = self.parse_json(raw_stream)
            watermarked = self.apply_watermark(parsed)
            deduped = self.deduplicate(watermarked)
            
            with_risk = self.add_risk_scores_udf(deduped)
            with_window = self.compute_windowed_aggregates(with_risk)
            with_velocity = self.compute_velocity_approximate(with_window)
            
            scored = self.compute_fraud_score(with_velocity)
            
            query = scored.writeStream \
                .outputMode("append") \
                .foreachBatch(self.write_to_mongodb) \
                .option("checkpointLocation", Config.SPARK_CHECKPOINT_STREAM) \
                .trigger(processingTime=Config.SPARK_TRIGGER_INTERVAL) \
                .start()
            
            agg_query = scored.writeStream \
                .outputMode("append") \
                .foreachBatch(self.write_aggregates_to_mongodb) \
                .option("checkpointLocation", f"{Config.SPARK_CHECKPOINT_STREAM}_agg") \
                .trigger(processingTime=Config.SPARK_TRIGGER_INTERVAL) \
                .start()
            
            console_query = self.write_to_console(scored)
            
            print("\n Streaming started. Press Ctrl+C to stop.\n")
            
            query.awaitTermination()
            
        except KeyboardInterrupt:
            print("\n Streaming stopped by user")
        except Exception as e:
            print(f" Streaming failed: {e}")
            raise
        finally:
            self.spark.stop()

def main():
    stream_job = FraudDetectionStream()
    stream_job.run()

if __name__ == "__main__":
    main()
