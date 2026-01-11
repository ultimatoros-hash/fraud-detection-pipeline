# Services Setup Guide

## 1. Create Kafka Topics

```powershell
# Create topics inside Kafka pod
kubectl exec -it kafka-0 -n fraud-detection -- kafka-topics.sh --create --topic transactions --bootstrap-server localhost:9092 --partitions 6 --replication-factor 1 --config retention.ms=604800000

kubectl exec -it kafka-0 -n fraud-detection -- kafka-topics.sh --create --topic fraud-alerts --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

kubectl exec -it kafka-0 -n fraud-detection -- kafka-topics.sh --list --bootstrap-server localhost:9092
```

---

## 2. Initialize MongoDB

```powershell
kubectl exec -it mongodb-0 -n fraud-detection -- mongosh
```

```javascript
use fraud_detection

// Create collections
db.createCollection("batch_views")
db.createCollection("realtime_views")
db.createCollection("fraud_alerts")
db.createCollection("user_profiles")
db.createCollection("dashboard_stats")

// Create indexes
db.batch_views.createIndex({ "user_id": 1, "timestamp": -1 })
db.batch_views.createIndex({ "is_fraud": 1, "timestamp": -1 })
db.realtime_views.createIndex({ "user_id": 1, "processing_time": -1 })
db.realtime_views.createIndex({ "processing_time": 1 }, { expireAfterSeconds: 3600 })
db.fraud_alerts.createIndex({ "timestamp": -1 })
db.fraud_alerts.createIndex({ "country": 1 })
db.dashboard_stats.createIndex({ "stat_type": 1, "timestamp": -1 })

// Insert metadata
db.system_metadata.insertOne({
  key: "last_batch_timestamp",
  value: new Date(),
  updated_at: new Date()
})

show collections
```

---

## 3. Setup Local Storage (HDFS Abstraction)

```powershell
$base = "c:\Projects\fraud-detection-pipeline\data"
@(
    "$base\raw\transactions\year=2025\month=01\day=10",
    "$base\raw\transactions\year=2025\month=01\day=11",
    "$base\master\batch_views",
    "$base\master\user_profiles"
) | ForEach-Object { New-Item -ItemType Directory -Force -Path $_ }

# Create static risk data
@'
{
  "country_risk": {"US": 0.1, "UK": 0.1, "NG": 0.8, "RU": 0.5, "UNKNOWN": 0.9},
  "merchant_risk": {"grocery": 0.1, "gambling": 0.85, "crypto": 0.9},
  "blacklist": ["MERCHANT_999"]
}
'@ | Out-File "$base\static\risk_data.json" -Encoding utf8
```

---

## 4. Verify Grafana

1. Open http://localhost:3000
2. Login: admin / admin
3. Go to Dashboards → Import
4. Import dashboards from `grafana/dashboards/`

---

## 5. Health Check

```powershell
# Quick connectivity test
python -c "
from kafka import KafkaConsumer
from pymongo import MongoClient
print('Kafka:', len(KafkaConsumer(bootstrap_servers='localhost:9092').topics()), 'topics')
print('MongoDB:', MongoClient('localhost:27017').list_database_names())
print('All services healthy')
"
```

---

## Running the Pipeline

```powershell
# Terminal 1: Producer
python src/producer/transaction_producer.py

# Terminal 2: Speed Layer
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 src/speed/fraud_detection_stream.py

# Terminal 3: Batch Layer (run periodically)
spark-submit src/batch/fraud_detection_batch.py

# Terminal 4: Dashboard
python src/dashboard/app.py
```
