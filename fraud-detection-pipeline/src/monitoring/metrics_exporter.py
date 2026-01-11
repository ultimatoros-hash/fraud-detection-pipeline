import os
import sys
import threading
import time
from typing import Dict, Optional

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

try:
    from prometheus_client import (
        Counter, Gauge, Histogram, Summary,
        start_http_server, CollectorRegistry, REGISTRY,
        generate_latest, CONTENT_TYPE_LATEST
    )
    from prometheus_client.core import GaugeMetricFamily, CounterMetricFamily
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False
    print(" prometheus_client not installed. Run: pip install prometheus-client")

from config.app_config import Config

if PROMETHEUS_AVAILABLE:
    TRANSACTIONS_TOTAL = Counter(
        'fraud_detection_transactions_total',
        'Total number of transactions processed',
        ['layer', 'status']
    )
    
    FRAUD_TRANSACTIONS = Counter(
        'fraud_detection_fraud_total',
        'Total number of fraudulent transactions detected',
        ['layer', 'country']
    )
    
    PROCESSING_LATENCY = Histogram(
        'fraud_detection_processing_latency_seconds',
        'Time taken to process transactions',
        ['layer'],
        buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
    )
    
    END_TO_END_LATENCY = Histogram(
        'fraud_detection_e2e_latency_seconds',
        'End-to-end latency from event time to processing',
        ['layer'],
        buckets=[0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0]
    )
    
    FRAUD_SCORE = Histogram(
        'fraud_detection_score',
        'Distribution of fraud scores',
        buckets=[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
    )
    
    ACTIVE_USERS = Gauge(
        'fraud_detection_active_users',
        'Number of unique active users in current window'
    )
    
    KAFKA_LAG = Gauge(
        'fraud_detection_kafka_consumer_lag',
        'Kafka consumer lag',
        ['partition']
    )
    
    BATCH_LAST_RUN = Gauge(
        'fraud_detection_batch_last_run_timestamp',
        'Timestamp of last batch run'
    )
    
    MODEL_AUC = Gauge(
        'fraud_detection_model_auc',
        'AUC score of current ML model'
    )
    
    MONGODB_DOCUMENT_COUNT = Gauge(
        'fraud_detection_mongodb_documents',
        'Number of documents in MongoDB collection',
        ['collection']
    )
    
    PROCESSING_LATENCY_SUMMARY = Summary(
        'fraud_detection_processing_latency_summary',
        'Processing latency summary with percentiles',
        ['layer']
    )

class MetricsCollector:
    
    def __init__(self, port: int = 8000):
        if not PROMETHEUS_AVAILABLE:
            raise ImportError("prometheus_client is required")
        
        self.port = port
        self._server_started = False
    
    def start_server(self):
        if not self._server_started:
            start_http_server(self.port)
            self._server_started = True
            print(f" Prometheus metrics available at http://localhost:{self.port}/metrics")
    
    def record_transaction(self, layer: str, success: bool = True):
        status = "success" if success else "error"
        TRANSACTIONS_TOTAL.labels(layer=layer, status=status).inc()
    
    def record_fraud(self, layer: str, country: str):
        FRAUD_TRANSACTIONS.labels(layer=layer, country=country).inc()
    
    def record_processing_latency(self, layer: str, latency_seconds: float):
        PROCESSING_LATENCY.labels(layer=layer).observe(latency_seconds)
        PROCESSING_LATENCY_SUMMARY.labels(layer=layer).observe(latency_seconds)
    
    def record_e2e_latency(self, layer: str, latency_seconds: float):
        END_TO_END_LATENCY.labels(layer=layer).observe(latency_seconds)
    
    def record_fraud_score(self, score: float):
        FRAUD_SCORE.observe(score)
    
    def set_active_users(self, count: int):
        ACTIVE_USERS.set(count)
    
    def set_kafka_lag(self, partition: int, lag: int):
        KAFKA_LAG.labels(partition=str(partition)).set(lag)
    
    def set_batch_timestamp(self, timestamp: float):
        BATCH_LAST_RUN.set(timestamp)
    
    def set_model_auc(self, auc: float):
        MODEL_AUC.set(auc)
    
    def set_mongodb_count(self, collection: str, count: int):
        MONGODB_DOCUMENT_COUNT.labels(collection=collection).set(count)

class MongoDBCollector:
    
    def __init__(self, mongodb_uri: str, database: str):
        try:
            from pymongo import MongoClient
            self.client = MongoClient(mongodb_uri)
            self.db = self.client[database]
        except ImportError:
            self.client = None
            self.db = None
    
    def collect(self):
        if not self.db:
            return
        
        collections = [
            Config.MONGODB_COLLECTION_BATCH_VIEWS,
            Config.MONGODB_COLLECTION_REALTIME_VIEWS,
            Config.MONGODB_COLLECTION_FRAUD_ALERTS,
            Config.MONGODB_COLLECTION_USER_PROFILES
        ]
        
        gauge = GaugeMetricFamily(
            'fraud_detection_mongodb_collection_size',
            'Document count per collection',
            labels=['collection']
        )
        
        for coll_name in collections:
            try:
                count = self.db[coll_name].count_documents({})
                gauge.add_metric([coll_name], count)
            except Exception:
                gauge.add_metric([coll_name], 0)
        
        yield gauge

class BackgroundMetricsUpdater:
    
    def __init__(self, collector: MetricsCollector, interval_seconds: int = 30):
        self.collector = collector
        self.interval = interval_seconds
        self._stop_event = threading.Event()
        self._thread = None
    
    def _update_mongodb_metrics(self):
        try:
            from pymongo import MongoClient
            client = MongoClient(Config.MONGODB_URI)
            db = client[Config.MONGODB_DATABASE]
            
            collections = [
                Config.MONGODB_COLLECTION_BATCH_VIEWS,
                Config.MONGODB_COLLECTION_REALTIME_VIEWS,
                Config.MONGODB_COLLECTION_FRAUD_ALERTS,
                Config.MONGODB_COLLECTION_USER_PROFILES
            ]
            
            for coll in collections:
                count = db[coll].count_documents({})
                self.collector.set_mongodb_count(coll, count)
            
            client.close()
        except Exception as e:
            print(f"Error updating MongoDB metrics: {e}")
    
    def _run(self):
        while not self._stop_event.is_set():
            try:
                self._update_mongodb_metrics()
            except Exception as e:
                print(f"Metrics update error: {e}")
            
            self._stop_event.wait(self.interval)
    
    def start(self):
        if self._thread is None or not self._thread.is_alive():
            self._stop_event.clear()
            self._thread = threading.Thread(target=self._run, daemon=True)
            self._thread.start()
            print(" Background metrics updater started")
    
    def stop(self):
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)

def create_metrics_flask_app():
    try:
        from flask import Flask, Response
        
        app = Flask(__name__)
        
        @app.route("/metrics")
        def metrics():
            return Response(
                generate_latest(REGISTRY),
                mimetype=CONTENT_TYPE_LATEST
            )
        
        @app.route("/health")
        def health():
            return {"status": "healthy"}
        
        return app
    except ImportError:
        print(" Flask not installed. Using default prometheus_client server.")
        return None

def main():
    if not PROMETHEUS_AVAILABLE:
        print(" prometheus_client is required")
        return
    
    print("=" * 60)
    print("PROMETHEUS METRICS EXPORTER")
    print("=" * 60)
    
    collector = MetricsCollector(port=8000)
    collector.start_server()
    
    updater = BackgroundMetricsUpdater(collector)
    updater.start()
    
    print("\nSimulating metrics...")
    
    import random
    
    try:
        while True:
            collector.record_transaction("stream", success=True)
            collector.record_processing_latency("stream", random.uniform(0.01, 0.5))
            
            if random.random() < 0.05:
                collector.record_fraud("stream", random.choice(["US", "NG", "RU", "BR"]))
            
            collector.record_fraud_score(random.uniform(0, 1))
            
            collector.set_active_users(random.randint(50, 200))
            
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\nStopping...")
        updater.stop()

if __name__ == "__main__":
    main()
