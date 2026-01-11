from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, IntegerType

COUNTRY_RISK_SCORES = {
    "US": 0.2, "UK": 0.2, "CA": 0.2, "DE": 0.3, "FR": 0.3,
    "NG": 0.9, "RU": 0.8, "CN": 0.6, "IN": 0.4, "BR": 0.5
}

MERCHANT_RISK_SCORES = {
    "grocery": 0.1, "restaurant": 0.2, "retail": 0.3,
    "electronics": 0.5, "jewelry": 0.7, "gambling": 0.9,
    "cryptocurrency": 0.95, "travel": 0.4, "entertainment": 0.3
}

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

def main():
    print("=" * 60)
    print("  SPARK BATCH FRAUD DETECTION JOB")
    print("=" * 60)
    
    spark = SparkSession.builder \
        .appName("FraudDetection-Batch") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    print(f" Spark {spark.version} connected")
    
    input_path = "/app/data/raw/transactions/*/*/*/*.jsonl"
    output_path = "/app/data/processed/spark_fraud_scores"
    
    print(f"\n Reading from: {input_path}")
    
    df = spark.read.schema(get_schema()).json(input_path)
    count = df.count()
    print(f"   Found {count:,} transactions")
    
    if count == 0:
        print("   No data to process")
        spark.stop()
        return
    
    country_cases = " ".join([f"WHEN country = '{c}' THEN {r}" for c, r in COUNTRY_RISK_SCORES.items()])
    merchant_cases = " ".join([f"WHEN merchant_category = '{m}' THEN {r}" for m, r in MERCHANT_RISK_SCORES.items()])
    
    df.createOrReplaceTempView("transactions")
    
    scored_df = spark.sql(f"""
        SELECT 
            *,
            LEAST(1.0,
                (CASE {country_cases} ELSE 0.5 END * 0.25) +
                (CASE {merchant_cases} ELSE 0.5 END * 0.20) +
                (CASE WHEN amount > 5000 THEN 0.20 WHEN amount > 1000 THEN 0.10 ELSE 0.0 END) +
                (CASE WHEN is_online = true THEN 0.10 ELSE 0.0 END) +
                (CASE WHEN hour_of_day BETWEEN 0 AND 5 THEN 0.10 ELSE 0.0 END) +
                (CASE WHEN is_fraud = true THEN 0.15 ELSE 0.0 END)
            ) as fraud_score,
            current_timestamp() as processing_time,
            'spark-batch' as processor
        FROM transactions
    """)
    
    final_df = scored_df.withColumn(
        "is_fraud_predicted",
        F.col("fraud_score") >= 0.6
    )
    
    print(f" Writing to: {output_path}")
    final_df.write \
        .mode("overwrite") \
        .partitionBy("is_fraud_predicted") \
        .parquet(output_path)
    
    summary = final_df.agg(
        F.count("*").alias("total"),
        F.sum(F.when(F.col("is_fraud_predicted"), 1).otherwise(0)).alias("fraud"),
        F.avg("fraud_score").alias("avg_score")
    ).collect()[0]
    
    print("\n" + "=" * 60)
    print(" BATCH PROCESSING RESULTS")
    print("=" * 60)
    print(f"   Total transactions:  {summary['total']:,}")
    print(f"   Fraud predicted:     {summary['fraud']:,}")
    print(f"   Fraud rate:          {summary['fraud']/summary['total']*100:.1f}%")
    print(f"   Average score:       {summary['avg_score']:.3f}")
    print("=" * 60)
    
    spark.stop()
    print("\n Job completed!")

if __name__ == "__main__":
    main()
