import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    BooleanType, TimestampType, IntegerType
)
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config.app_config import Config, COUNTRY_RISK_SCORES, MERCHANT_RISK_SCORES

class FraudDetectionBatch:
    
    def __init__(self):
        self.spark = self._create_spark_session()
        self.batch_timestamp = datetime.utcnow()
        
        self.country_risk_bc = self.spark.sparkContext.broadcast(COUNTRY_RISK_SCORES)
        self.merchant_risk_bc = self.spark.sparkContext.broadcast(MERCHANT_RISK_SCORES)
        
        print(f" Batch Layer initialized at {self.batch_timestamp}")
    
    def _create_spark_session(self) -> SparkSession:
        return SparkSession.builder \
            .appName(Config.SPARK_APP_NAME_BATCH) \
            .master(Config.SPARK_MASTER) \
            .config("spark.sql.shuffle.partitions", Config.SPARK_BATCH_PARTITIONS) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.parquet.compression.codec", "snappy") \
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
    
    def read_master_dataset(self) -> DataFrame:
        print(f" Reading master dataset from: {Config.HDFS_MASTER_DATASET}")
        
        df = self.spark.read \
            .schema(self.get_transaction_schema()) \
            .json(f"{Config.HDFS_MASTER_DATASET}/*/*/*/*/*.jsonl")
        
        df = df.withColumn("event_timestamp", F.to_timestamp("event_time")) \
               .withColumn("processing_timestamp", F.to_timestamp("timestamp"))
        
        df = df.dropDuplicates(["transaction_id"])
        
        record_count = df.count()
        print(f" Loaded {record_count:,} transactions")
        
        return df
    
    def compute_user_profiles(self, df: DataFrame) -> DataFrame:
        print(" Computing user profiles...")
        
        user_window = Window.partitionBy("user_id")
        
        user_profiles = df.groupBy("user_id").agg(
            F.count("*").alias("total_transactions"),
            F.sum("amount").alias("total_amount"),
            F.avg("amount").alias("avg_amount"),
            F.stddev("amount").alias("stddev_amount"),
            F.min("event_timestamp").alias("first_transaction"),
            F.max("event_timestamp").alias("last_transaction"),
            F.countDistinct("country").alias("unique_countries"),
            F.countDistinct("merchant_id").alias("unique_merchants"),
            F.sum(F.when(F.col("is_online"), 1).otherwise(0)).alias("online_count"),
            F.sum(F.when(F.col("_label") == "fraud", 1).otherwise(0)).alias("fraud_count"),
            F.collect_set("country").alias("countries_used"),
            F.collect_set("merchant_category").alias("categories_used")
        )
        
        user_profiles = user_profiles \
            .withColumn("fraud_rate", 
                F.col("fraud_count") / F.col("total_transactions")) \
            .withColumn("online_rate", 
                F.col("online_count") / F.col("total_transactions")) \
            .withColumn("account_age_days",
                F.datediff(F.current_timestamp(), F.col("first_transaction")))
        
        user_profiles.cache()
        
        print(f" Computed profiles for {user_profiles.count():,} users")
        return user_profiles
    
    def add_risk_scores(self, df: DataFrame) -> DataFrame:
        print(" Adding risk scores via broadcast join...")
        
        country_risk_df = self.spark.createDataFrame(
            [(k, v) for k, v in COUNTRY_RISK_SCORES.items()],
            ["country", "country_risk"]
        )
        
        merchant_risk_df = self.spark.createDataFrame(
            [(k, v) for k, v in MERCHANT_RISK_SCORES.items()],
            ["merchant_category", "merchant_risk"]
        )
        
        df = df.join(F.broadcast(country_risk_df), "country", "left") \
               .join(F.broadcast(merchant_risk_df), "merchant_category", "left")
        
        df = df.fillna({"country_risk": 0.9, "merchant_risk": 0.5})
        
        return df
    
    def compute_velocity_features(self, df: DataFrame) -> DataFrame:
        print(" Computing velocity features...")
        
        velocity_window = Window.partitionBy("user_id") \
            .orderBy(F.col("event_timestamp").cast("long")) \
            .rangeBetween(-300, 0)
        
        prev_window = Window.partitionBy("user_id") \
            .orderBy("event_timestamp")
        
        df = df \
            .withColumn("velocity_count", F.count("*").over(velocity_window)) \
            .withColumn("velocity_amount", F.sum("amount").over(velocity_window)) \
            .withColumn("prev_timestamp", F.lag("event_timestamp").over(prev_window)) \
            .withColumn("time_since_last",
                F.when(F.col("prev_timestamp").isNull(), 999999)
                 .otherwise(
                    F.unix_timestamp("event_timestamp") - 
                    F.unix_timestamp("prev_timestamp")
                 ))
        
        return df
    
    def compute_amount_features(self, df: DataFrame, user_profiles: DataFrame) -> DataFrame:
        print(" Computing amount features...")
        
        df = df.join(
            user_profiles.select("user_id", "avg_amount", "stddev_amount"),
            "user_id",
            "left"
        )
        
        df = df.withColumn("amount_zscore",
            F.when(F.col("stddev_amount") > 0,
                (F.col("amount") - F.col("avg_amount")) / F.col("stddev_amount")
            ).otherwise(0)
        )
        
        df = df \
            .withColumn("is_high_amount", 
                F.col("amount") > Config.HIGH_AMOUNT_THRESHOLD) \
            .withColumn("is_very_high_amount",
                F.col("amount") > Config.VERY_HIGH_AMOUNT_THRESHOLD)
        
        return df
    
    def compute_fraud_score(self, df: DataFrame) -> DataFrame:
        print(" Computing fraud scores...")
        
        df = df.withColumn("fraud_score",
            (F.col("country_risk") * 0.25) +
            (F.col("merchant_risk") * 0.20) +
            (F.when(F.col("amount_zscore") > 3, 0.2)
              .when(F.col("amount_zscore") > 2, 0.1)
              .otherwise(0)) +
            (F.when(F.col("velocity_count") > Config.VELOCITY_COUNT_THRESHOLD, 0.15)
              .otherwise(0)) +
            (F.when(F.col("velocity_amount") > Config.VELOCITY_AMOUNT_THRESHOLD, 0.1)
              .otherwise(0)) +
            (F.when(F.col("is_online"), 0.05).otherwise(0)) +
            (F.when((F.col("hour_of_day") >= 0) & (F.col("hour_of_day") <= 5), 0.05)
              .otherwise(0))
        )
        
        df = df.withColumn("fraud_score",
            F.when(F.col("fraud_score") > 1.0, 1.0)
             .when(F.col("fraud_score") < 0.0, 0.0)
             .otherwise(F.col("fraud_score"))
        )
        
        df = df.withColumn("is_fraud_predicted",
            F.col("fraud_score") >= Config.FRAUD_THRESHOLD
        )
        
        return df
    
    def train_ml_model(self, df: DataFrame):
        print(" Training ML model...")
        
        training_df = df.filter(F.col("_label").isin(["normal", "fraud"])) \
            .withColumn("label", 
                F.when(F.col("_label") == "fraud", 1.0).otherwise(0.0))
        
        if training_df.count() < 100:
            print(" Insufficient training data. Skipping ML training.")
            return None
        
        feature_cols = [
            "amount", "country_risk", "merchant_risk",
            "velocity_count", "velocity_amount", "hour_of_day"
        ]
        
        for col in feature_cols:
            training_df = training_df.fillna({col: 0.0})
        
        assembler = VectorAssembler(
            inputCols=feature_cols,
            outputCol="features_raw"
        )
        
        scaler = StandardScaler(
            inputCol="features_raw",
            outputCol="features",
            withStd=True,
            withMean=True
        )
        
        lr = LogisticRegression(
            featuresCol="features",
            labelCol="label",
            maxIter=Config.ML_MAX_ITER,
            regParam=Config.ML_REG_PARAM
        )
        
        pipeline = Pipeline(stages=[assembler, scaler, lr])
        
        train_data, test_data = training_df.randomSplit(
            [Config.ML_TRAIN_TEST_SPLIT, 1 - Config.ML_TRAIN_TEST_SPLIT],
            seed=42
        )
        
        model = pipeline.fit(train_data)
        
        predictions = model.transform(test_data)
        evaluator = BinaryClassificationEvaluator(
            labelCol="label",
            rawPredictionCol="rawPrediction",
            metricName="areaUnderROC"
        )
        auc = evaluator.evaluate(predictions)
        print(f" Model AUC-ROC: {auc:.4f}")
        
        model_path = os.path.join(Config.ML_MODEL_PATH, f"model_{self.batch_timestamp.strftime('%Y%m%d_%H%M%S')}")
        model.write().overwrite().save(model_path)
        print(f" Model saved to: {model_path}")
        
        return model
    
    def generate_batch_views(self, df: DataFrame, user_profiles: DataFrame):
        print(" Generating batch views...")
        
        fraud_results = df.select(
            "transaction_id", "user_id", "amount", "country",
            "merchant_category", "fraud_score", "is_fraud_predicted",
            "event_timestamp", "country_risk", "merchant_risk",
            "velocity_count", "amount_zscore"
        ).withColumn("batch_timestamp", F.lit(self.batch_timestamp))
        
        fraud_output = os.path.join(Config.HDFS_BATCH_OUTPUT, "fraud_results")
        fraud_results.write.mode("overwrite").parquet(fraud_output)
        print(f" Fraud results: {fraud_output}")
        
        profiles_output = os.path.join(Config.HDFS_BATCH_OUTPUT, "user_profiles")
        user_profiles.write.mode("overwrite").parquet(profiles_output)
        print(f" User profiles: {profiles_output}")
        
        daily_stats = df.groupBy(
            F.date_trunc("day", "event_timestamp").alias("date")
        ).agg(
            F.count("*").alias("total_transactions"),
            F.sum("amount").alias("total_amount"),
            F.sum(F.when(F.col("is_fraud_predicted"), 1).otherwise(0)).alias("fraud_count"),
            F.avg("fraud_score").alias("avg_fraud_score")
        ).withColumn("batch_timestamp", F.lit(self.batch_timestamp))
        
        stats_output = os.path.join(Config.HDFS_BATCH_OUTPUT, "daily_stats")
        daily_stats.write.mode("overwrite").parquet(stats_output)
        print(f" Daily stats: {stats_output}")
        
        country_stats = df.groupBy("country").agg(
            F.count("*").alias("total_transactions"),
            F.sum("amount").alias("total_amount"),
            F.sum(F.when(F.col("is_fraud_predicted"), 1).otherwise(0)).alias("fraud_count"),
            F.avg("fraud_score").alias("avg_fraud_score")
        ).withColumn("fraud_rate", 
            F.col("fraud_count") / F.col("total_transactions")
        )
        
        country_output = os.path.join(Config.HDFS_BATCH_OUTPUT, "country_stats")
        country_stats.write.mode("overwrite").parquet(country_output)
        print(f" Country stats: {country_output}")
        
        return {
            "fraud_results": fraud_output,
            "user_profiles": profiles_output,
            "daily_stats": stats_output,
            "country_stats": country_output
        }
    
    def write_to_mongodb(self, df: DataFrame, user_profiles: DataFrame):
        print(" Writing to MongoDB...")
        
        try:
            from pymongo import MongoClient
            client = MongoClient(Config.MONGODB_URI)
            db = client[Config.MONGODB_DATABASE]
            
            fraud_alerts = df.filter(F.col("is_fraud_predicted") == True) \
                .limit(10000).toPandas()
            
            if len(fraud_alerts) > 0:
                fraud_alerts["batch_timestamp"] = self.batch_timestamp
                db[Config.MONGODB_COLLECTION_BATCH_VIEWS].delete_many({})
                db[Config.MONGODB_COLLECTION_BATCH_VIEWS].insert_many(
                    fraud_alerts.to_dict("records")
                )
                print(f" Wrote {len(fraud_alerts)} fraud alerts to MongoDB")
            
            profiles_pd = user_profiles.limit(10000).toPandas()
            if len(profiles_pd) > 0:
                db[Config.MONGODB_COLLECTION_USER_PROFILES].delete_many({})
                db[Config.MONGODB_COLLECTION_USER_PROFILES].insert_many(
                    profiles_pd.to_dict("records")
                )
                print(f" Wrote {len(profiles_pd)} user profiles to MongoDB")
            
            db["system_metadata"].update_one(
                {"key": "last_batch_timestamp"},
                {"$set": {"value": self.batch_timestamp, "updated_at": datetime.utcnow()}},
                upsert=True
            )
            
            client.close()
            
        except Exception as e:
            print(f" MongoDB write failed: {e}")
    
    def explain_execution_plan(self, df: DataFrame):
        print("\n" + "=" * 60)
        print("EXECUTION PLAN ANALYSIS")
        print("=" * 60)
        df.explain(mode="extended")
        print("=" * 60 + "\n")
    
    def run(self):
        print("\n" + "=" * 60)
        print("BATCH LAYER - FRAUD DETECTION")
        print(f"Timestamp: {self.batch_timestamp}")
        print("=" * 60 + "\n")
        
        start_time = datetime.now()
        
        try:
            df = self.read_master_dataset()
            
            user_profiles = self.compute_user_profiles(df)
            
            df = self.add_risk_scores(df)
            
            df = self.compute_velocity_features(df)
            
            df = self.compute_amount_features(df, user_profiles)
            
            df = self.compute_fraud_score(df)
            
            df.cache()
            
            model = self.train_ml_model(df)
            
            outputs = self.generate_batch_views(df, user_profiles)
            
            self.write_to_mongodb(df, user_profiles)
            
            self.explain_execution_plan(df)
            
            total = df.count()
            fraud_count = df.filter(F.col("is_fraud_predicted")).count()
            
            elapsed = (datetime.now() - start_time).total_seconds()
            
            print("\n" + "=" * 60)
            print("BATCH PROCESSING COMPLETE")
            print("=" * 60)
            print(f"Total transactions processed: {total:,}")
            print(f"Fraud predictions: {fraud_count:,} ({fraud_count/max(1,total)*100:.2f}%)")
            print(f"Processing time: {elapsed:.2f} seconds")
            print(f"Throughput: {total/max(1,elapsed):.0f} records/sec")
            print("=" * 60)
            
        except Exception as e:
            print(f" Batch processing failed: {e}")
            raise
        finally:
            self.spark.stop()

def main():
    batch_job = FraudDetectionBatch()
    batch_job.run()

if __name__ == "__main__":
    main()
