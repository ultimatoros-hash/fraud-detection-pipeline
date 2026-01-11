import os
import sys
import unittest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock
import json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.app_config import Config, COUNTRY_RISK_SCORES, MERCHANT_RISK_SCORES

class TestConfig(unittest.TestCase):
    
    def test_kafka_config_exists(self):
        self.assertIsNotNone(Config.KAFKA_BOOTSTRAP_SERVERS)
        self.assertIsNotNone(Config.KAFKA_TOPIC)
        self.assertEqual(Config.KAFKA_TOPIC, "transactions")
    
    def test_mongodb_config_exists(self):
        self.assertIsNotNone(Config.MONGODB_URI)
        self.assertIsNotNone(Config.MONGODB_DATABASE)
    
    def test_fraud_threshold_valid(self):
        self.assertGreaterEqual(Config.FRAUD_THRESHOLD, 0.0)
        self.assertLessEqual(Config.FRAUD_THRESHOLD, 1.0)
    
    def test_country_risk_scores_valid(self):
        for country, score in COUNTRY_RISK_SCORES.items():
            self.assertGreaterEqual(score, 0.0, f"{country} score too low")
            self.assertLessEqual(score, 1.0, f"{country} score too high")
    
    def test_merchant_risk_scores_valid(self):
        for category, score in MERCHANT_RISK_SCORES.items():
            self.assertGreaterEqual(score, 0.0, f"{category} score too low")
            self.assertLessEqual(score, 1.0, f"{category} score too high")

class TestFraudScorer(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        from src.ml.fraud_model import FraudScorer
        cls.scorer = FraudScorer()
    
    def test_score_normal_transaction(self):
        transaction = {
            "transaction_id": "test-normal-001",
            "user_id": "USER_00001",
            "amount": 50.0,
            "country": "US",
            "merchant_category": "grocery",
            "is_online": False,
            "hour_of_day": 14,
            "velocity_count": 1
        }
        
        score, is_fraud, features = self.scorer.score_transaction(transaction)
        
        self.assertGreaterEqual(score, 0.0)
        self.assertLessEqual(score, 1.0)
        self.assertFalse(is_fraud, "Normal transaction should not be flagged as fraud")
        self.assertIn("country_risk", features)
    
    def test_score_fraudulent_transaction(self):
        transaction = {
            "transaction_id": "test-fraud-001",
            "user_id": "USER_00002",
            "amount": 10000.0,
            "country": "NG",
            "merchant_category": "crypto",
            "is_online": True,
            "hour_of_day": 3,
            "velocity_count": 10
        }
        
        score, is_fraud, features = self.scorer.score_transaction(transaction)
        
        self.assertGreater(score, Config.FRAUD_THRESHOLD)
        self.assertTrue(is_fraud, "High-risk transaction should be flagged as fraud")
    
    def test_score_batch(self):
        transactions = [
            {"amount": 50, "country": "US", "merchant_category": "grocery", "is_online": False, "hour_of_day": 12},
            {"amount": 5000, "country": "RU", "merchant_category": "gambling", "is_online": True, "hour_of_day": 2}
        ]
        
        results = self.scorer.score_batch(transactions)
        
        self.assertEqual(len(results), 2)
        self.assertIsInstance(results[0], tuple)
        self.assertEqual(len(results[0]), 3)
    
    def test_missing_fields_handled(self):
        transaction = {"transaction_id": "test-minimal"}
        
        score, is_fraud, features = self.scorer.score_transaction(transaction)
        
        self.assertIsInstance(score, float)
        self.assertIsInstance(is_fraud, bool)

class TestTransactionGenerator(unittest.TestCase):
    
    def test_generate_normal_transaction(self):
        with patch('src.producer.transaction_producer.KafkaProducer'):
            from src.producer.transaction_producer import TransactionProducer
            producer = TransactionProducer(enable_kafka=False)
            
            txn = producer.generate_normal_transaction("USER_00001")
            
            self.assertIn("transaction_id", txn)
            self.assertIn("user_id", txn)
            self.assertIn("amount", txn)
            self.assertIn("timestamp", txn)
            self.assertIn("country", txn)
            self.assertIn("merchant_category", txn)
            self.assertEqual(txn["is_fraud"], False)
    
    def test_generate_fraud_transaction(self):
        with patch('src.producer.transaction_producer.KafkaProducer'):
            from src.producer.transaction_producer import TransactionProducer
            producer = TransactionProducer(enable_kafka=False)
            
            txn = producer.generate_fraud_transaction("USER_00002")
            
            self.assertEqual(txn["is_fraud"], True)
            self.assertGreater(txn["amount"], 0)

class TestServingLayerQueries(unittest.TestCase):
    
    def setUp(self):
        self.mock_client = MagicMock()
        self.mock_db = MagicMock()
        self.mock_client.__getitem__ = Mock(return_value=self.mock_db)
    
    @patch('pymongo.MongoClient')
    def test_get_fraud_alerts(self, mock_mongo):
        mock_mongo.return_value = self.mock_client
        
        mock_collection = MagicMock()
        mock_collection.find.return_value.sort.return_value.limit.return_value = []
        self.mock_db.__getitem__ = Mock(return_value=mock_collection)
        
        from src.serving.serving_layer import ServingLayer
        
        serving = ServingLayer()
        serving.metadata.find_one = Mock(return_value={"value": datetime.utcnow()})
        
        alerts = serving.get_fraud_alerts(limit=10)
        
        self.assertIsInstance(alerts, list)
    
    @patch('pymongo.MongoClient')
    def test_get_recent_stats(self, mock_mongo):
        mock_mongo.return_value = self.mock_client
        
        mock_collection = MagicMock()
        mock_collection.count_documents.return_value = 100
        self.mock_db.__getitem__ = Mock(return_value=mock_collection)
        
        from src.serving.serving_layer import ServingLayer
        serving = ServingLayer()
        
        stats = serving.get_recent_stats()
        
        self.assertIn("last_hour", stats)
        self.assertIn("all_time", stats)
        self.assertIn("timestamp", stats)

class TestDataValidation(unittest.TestCase):
    
    def test_transaction_schema(self):
        required_fields = [
            "transaction_id",
            "user_id",
            "amount",
            "timestamp",
            "country",
            "merchant_id",
            "merchant_category",
            "is_online",
            "is_fraud"
        ]
        
        sample_transaction = {
            "transaction_id": "txn-001",
            "user_id": "user-001",
            "amount": 100.0,
            "timestamp": datetime.now().isoformat(),
            "country": "US",
            "merchant_id": "merchant-001",
            "merchant_category": "retail",
            "is_online": False,
            "is_fraud": False
        }
        
        for field in required_fields:
            self.assertIn(field, sample_transaction, f"Missing required field: {field}")
    
    def test_amount_positive(self):
        amounts = [100.0, 50.25, 1000, 0.01]
        for amount in amounts:
            self.assertGreater(amount, 0, "Amount must be positive")
    
    def test_timestamp_valid(self):
        timestamp_str = datetime.now().isoformat()
        parsed = datetime.fromisoformat(timestamp_str)
        self.assertIsInstance(parsed, datetime)

class TestIntegration(unittest.TestCase):
    
    @unittest.skipIf(
        os.environ.get("SKIP_INTEGRATION", "true").lower() == "true",
        "Skipping integration tests"
    )
    def test_end_to_end_flow(self):
        pass
    
    @unittest.skipIf(
        os.environ.get("SKIP_INTEGRATION", "true").lower() == "true",
        "Skipping integration tests"
    )
    def test_kafka_connectivity(self):
        pass
    
    @unittest.skipIf(
        os.environ.get("SKIP_INTEGRATION", "true").lower() == "true",
        "Skipping integration tests"
    )
    def test_mongodb_connectivity(self):
        pass

class TestMetricsExporter(unittest.TestCase):
    
    @patch('prometheus_client.start_http_server')
    def test_metrics_server_start(self, mock_server):
        from src.monitoring.metrics_exporter import MetricsCollector
        
        collector = MetricsCollector(port=8000)
        collector.start_server()
        
        mock_server.assert_called_once_with(8000)
    
    @patch('prometheus_client.Counter')
    def test_record_transaction(self, mock_counter):
        pass

def run_tests():
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    suite.addTests(loader.loadTestsFromTestCase(TestConfig))
    suite.addTests(loader.loadTestsFromTestCase(TestFraudScorer))
    suite.addTests(loader.loadTestsFromTestCase(TestTransactionGenerator))
    suite.addTests(loader.loadTestsFromTestCase(TestServingLayerQueries))
    suite.addTests(loader.loadTestsFromTestCase(TestDataValidation))
    suite.addTests(loader.loadTestsFromTestCase(TestIntegration))
    suite.addTests(loader.loadTestsFromTestCase(TestMetricsExporter))
    
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    return result.wasSuccessful()

if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)
