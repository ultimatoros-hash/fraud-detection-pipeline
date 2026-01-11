import os
import sys

os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jre1.8.0_471"
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = os.environ["HADOOP_HOME"] + r"\bin;" + os.environ.get("PATH", "")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, IntegerType
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config.app_config import COUNTRY_RISK_SCORES, MERCHANT_RISK_SCORES

def create_spark_session():
    return SparkSession.builder \
        .appName("FraudDetection-Batch") \
        .master("local[1]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.driver.memory", "1g") \
        .config("spark.ui.enabled", "false") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
        .getOrCreate()

def get_schema():
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

def process_batch_file(spark, input_path: str, output_path: str):
    print(f"\n Reading data from: {input_path}")
    
    df = spark.read \
        .schema(get_schema()) \
        .json(input_path)
    
    count = df.count()
    print(f"   Found {count:,} transactions")
    
    if count == 0:
        print("   No data to process")
        return
    
    print(" Calculating fraud scores with Spark SQL...")
    
    country_cases = " ".join([
        f"WHEN country = '{c}' THEN {r}" 
        for c, r in COUNTRY_RISK_SCORES.items()
    ])
    
    merchant_cases = " ".join([
        f"WHEN merchant_category = '{m}' THEN {r}" 
        for m, r in MERCHANT_RISK_SCORES.items()
    ])
    
    df.createOrReplaceTempView("transactions")
    
    scored_df = spark.sql(f"""
        SELECT 
            *,
            -- Country risk (25% weight)
            CASE {country_cases} ELSE 0.5 END * 0.25 as country_component,
            
            -- Merchant risk (20% weight)  
            CASE {merchant_cases} ELSE 0.5 END * 0.20 as merchant_component,
            
            -- Amount risk (20% weight)
            CASE 
                WHEN amount > 5000 THEN 0.20
                WHEN amount > 1000 THEN 0.10
                ELSE 0.0 
            END as amount_component,
            
            -- Online risk (10% weight)
            CASE WHEN is_online = true THEN 0.10 ELSE 0.0 END as online_component,
            
            -- Hour risk (10% weight)
            CASE WHEN hour_of_day BETWEEN 0 AND 5 THEN 0.10 ELSE 0.0 END as hour_component,
            
            -- Known fraud boost
            CASE WHEN is_fraud = true THEN 0.15 ELSE 0.0 END as fraud_boost,
            
            current_timestamp() as processing_time,
            'spark-batch' as processor
        FROM transactions
    """)
    
    final_df = scored_df.withColumn(
        "fraud_score",
        F.least(
            F.lit(1.0),
            F.col("country_component") + 
            F.col("merchant_component") + 
            F.col("amount_component") +
            F.col("online_component") +
            F.col("hour_component") +
            F.col("fraud_boost")
        )
    ).withColumn(
        "is_fraud_predicted",
        F.col("fraud_score") >= 0.6
    ).drop(
        "country_component", "merchant_component", "amount_component",
        "online_component", "hour_component", "fraud_boost"
    )
    
    print(f" Writing results to: {output_path}")
    final_df.write \
        .mode("overwrite") \
        .partitionBy("is_fraud_predicted") \
        .parquet(output_path)
    
    summary = final_df.agg(
        F.count("*").alias("total"),
        F.sum(F.when(F.col("is_fraud_predicted") == True, 1).otherwise(0)).alias("fraud_count"),
        F.avg("fraud_score").alias("avg_score")
    ).collect()[0]
    
    print("\n" + "=" * 50)
    print(" BATCH PROCESSING SUMMARY")
    print("=" * 50)
    print(f"   Total transactions: {summary['total']:,}")
    print(f"   Fraud predicted:    {summary['fraud_count']:,}")
    print(f"   Fraud rate:         {summary['fraud_count']/summary['total']*100:.1f}%")
    print(f"   Average score:      {summary['avg_score']:.3f}")
    print("=" * 50)

def main():
    print("=" * 60)
    print("  SPARK BATCH FRAUD DETECTION")
    print("=" * 60)
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    print(f" Spark {spark.version} initialized")
    
    input_path = "data/raw/transactions/*/*/*/*.jsonl"
    output_path = "data/processed/fraud_scores"
    
    try:
        process_batch_file(spark, input_path, output_path)
        print("\n Batch processing complete!")
        print(f"   Results saved to: {output_path}")
        
    except Exception as e:
        print(f"\n Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()
        print("\n Spark session stopped")

if __name__ == "__main__":
    main()
