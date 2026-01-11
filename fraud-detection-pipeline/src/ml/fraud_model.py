import os
import sys
from typing import Dict, List, Optional, Tuple
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from config.app_config import Config, COUNTRY_RISK_SCORES, MERCHANT_RISK_SCORES

class FraudScorer:
    
    def __init__(self):
        self.country_risk = COUNTRY_RISK_SCORES
        self.merchant_risk = MERCHANT_RISK_SCORES
        self.threshold = Config.FRAUD_THRESHOLD
    
    def score_transaction(self, transaction: Dict) -> Tuple[float, bool, Dict]:
        features = {}
        score = 0.0
        
        country = transaction.get("country", "UNKNOWN")
        country_risk = self.country_risk.get(country, 0.9)
        features["country_risk"] = country_risk
        score += country_risk * 0.25
        
        category = transaction.get("merchant_category", "unknown")
        merchant_risk = self.merchant_risk.get(category, 0.5)
        features["merchant_risk"] = merchant_risk
        score += merchant_risk * 0.20
        
        amount = transaction.get("amount", 0)
        if amount > 5000:
            features["amount_risk"] = 1.0
            score += 0.20
        elif amount > 1000:
            features["amount_risk"] = 0.5
            score += 0.10
        else:
            features["amount_risk"] = 0.0
        
        is_online = transaction.get("is_online", False)
        features["is_online"] = 1.0 if is_online else 0.0
        if is_online:
            score += 0.10
        
        hour = transaction.get("hour_of_day", 12)
        if 0 <= hour <= 5:
            features["hour_risk"] = 1.0
            score += 0.10
        else:
            features["hour_risk"] = 0.0
        
        velocity = transaction.get("velocity_count", 0)
        if velocity > Config.VELOCITY_COUNT_THRESHOLD:
            features["velocity_risk"] = 1.0
            score += 0.15
        else:
            features["velocity_risk"] = 0.0
        
        score = max(0.0, min(1.0, score))
        is_fraud = score >= self.threshold
        
        return score, is_fraud, features
    
    def score_batch(self, transactions: List[Dict]) -> List[Tuple[float, bool, Dict]]:
        return [self.score_transaction(t) for t in transactions]

class ModelTrainer:
    
    def __init__(self, spark_session=None):
        self.spark = spark_session
        self.model = None
        self.model_path = Config.ML_MODEL_PATH
    
    def prepare_features(self, df):
        from pyspark.ml.feature import VectorAssembler, StandardScaler
        
        feature_cols = [
            "amount", "country_risk", "merchant_risk",
            "is_online_flag", "hour_of_day", "velocity_count"
        ]
        
        for col in feature_cols:
            df = df.fillna({col: 0.0})
        
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
        
        return assembler, scaler
    
    def train(self, training_df):
        from pyspark.ml.classification import LogisticRegression
        from pyspark.ml import Pipeline
        from pyspark.ml.evaluation import BinaryClassificationEvaluator
        
        assembler, scaler = self.prepare_features(training_df)
        
        lr = LogisticRegression(
            featuresCol="features",
            labelCol="label",
            maxIter=Config.ML_MAX_ITER,
            regParam=Config.ML_REG_PARAM
        )
        
        pipeline = Pipeline(stages=[assembler, scaler, lr])
        
        train_data, test_data = training_df.randomSplit([0.8, 0.2], seed=42)
        
        self.model = pipeline.fit(train_data)
        
        predictions = self.model.transform(test_data)
        evaluator = BinaryClassificationEvaluator(
            labelCol="label",
            rawPredictionCol="rawPrediction",
            metricName="areaUnderROC"
        )
        auc = evaluator.evaluate(predictions)
        
        return {"auc": auc, "model": self.model}
    
    def save_model(self, path: str = None):
        if not self.model:
            raise ValueError("No model to save. Train first.")
        
        path = path or os.path.join(
            self.model_path,
            f"model_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        )
        
        self.model.write().overwrite().save(path)
        return path
    
    def load_model(self, path: str):
        from pyspark.ml import PipelineModel
        self.model = PipelineModel.load(path)
        return self.model
    
    def predict(self, df):
        if not self.model:
            raise ValueError("No model loaded. Train or load first.")
        return self.model.transform(df)

def compute_user_features(transactions_df, spark):
    from pyspark.sql import functions as F
    
    return transactions_df.groupBy("user_id").agg(
        F.count("*").alias("transaction_count"),
        F.avg("amount").alias("avg_amount"),
        F.stddev("amount").alias("stddev_amount"),
        F.sum("amount").alias("total_amount"),
        F.countDistinct("country").alias("unique_countries"),
        F.countDistinct("merchant_id").alias("unique_merchants"),
        F.sum(F.when(F.col("is_online"), 1).otherwise(0)).alias("online_count")
    )

def compute_velocity_features(transactions_df, window_minutes: int = 5):
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    
    window_seconds = window_minutes * 60
    
    velocity_window = Window.partitionBy("user_id") \
        .orderBy(F.col("event_timestamp").cast("long")) \
        .rangeBetween(-window_seconds, 0)
    
    return transactions_df \
        .withColumn("velocity_count", F.count("*").over(velocity_window)) \
        .withColumn("velocity_amount", F.sum("amount").over(velocity_window))

def compute_amount_zscore(transactions_df, user_stats_df):
    from pyspark.sql import functions as F
    
    joined = transactions_df.join(
        user_stats_df.select("user_id", "avg_amount", "stddev_amount"),
        "user_id",
        "left"
    )
    
    return joined.withColumn("amount_zscore",
        F.when(F.col("stddev_amount") > 0,
            (F.col("amount") - F.col("avg_amount")) / F.col("stddev_amount")
        ).otherwise(0)
    )

def main():
    print("=" * 60)
    print("ML MODULE TEST")
    print("=" * 60)
    
    scorer = FraudScorer()
    
    test_transactions = [
        {
            "transaction_id": "test-1",
            "user_id": "USER_00001",
            "amount": 50.0,
            "country": "US",
            "merchant_category": "grocery",
            "is_online": False,
            "hour_of_day": 14
        },
        {
            "transaction_id": "test-2",
            "user_id": "USER_00002",
            "amount": 5000.0,
            "country": "NG",
            "merchant_category": "crypto",
            "is_online": True,
            "hour_of_day": 3
        }
    ]
    
    print("\nScoring test transactions:")
    for txn in test_transactions:
        score, is_fraud, features = scorer.score_transaction(txn)
        print(f"\n  Transaction: {txn['transaction_id']}")
        print(f"  Amount: ${txn['amount']:.2f}, Country: {txn['country']}")
        print(f"  Fraud Score: {score:.2f}")
        print(f"  Is Fraud: {is_fraud}")
        print(f"  Features: {features}")
    
    print("\n ML module test complete")

if __name__ == "__main__":
    main()
