# Fraud Detection Pipeline - Quick Start Guide

## Prerequisites

- Docker Desktop installed and running
- Python 3.9+ installed
- Git (optional)

---

## Step 1: Install Python Dependencies

```powershell
cd C:\Projects\fraud-detection-pipeline
pip install -r requirements.txt
```

---

## Step 2: Start Docker Services

```powershell
docker-compose up -d
```

Wait 30-60 seconds for all services to initialize.

### Verify Services Are Running

```powershell
docker ps
```

Expected containers:
- fraud-kafka
- fraud-zookeeper
- fraud-mongodb
- fraud-kafka-ui
- fraud-mongo-express
- fraud-prometheus
- fraud-grafana
- fraud-spark-master
- fraud-spark-worker

---

## Step 3: Create Kafka Topic (First Time Only)

```powershell
docker exec fraud-kafka kafka-topics --bootstrap-server localhost:9092 --create --topic transactions --partitions 6 --replication-factor 1
```

---

## Step 4: Run the Pipeline

Open 3 separate PowerShell terminals:

### Terminal 1 - Transaction Producer
```powershell
cd C:\Projects\fraud-detection-pipeline
python src/producer/transaction_producer.py
```

### Terminal 2 - Fraud Processor (Speed Layer)
```powershell
cd C:\Projects\fraud-detection-pipeline
python src/speed/simple_processor.py
```

### Terminal 3 - Dashboard
```powershell
cd C:\Projects\fraud-detection-pipeline
python src/dashboard/pro_dashboard.py
```

---

## Step 5: Access the Applications

| Application | URL | Credentials |
|-------------|-----|-------------|
| Dashboard | http://localhost:8050 | - |
| Kafka UI | http://localhost:8080 | - |
| Mongo Express | http://localhost:8081 | admin / admin123 |
| Grafana | http://localhost:3000 | admin / admin123 |
| Prometheus | http://localhost:9090 | - |
| Spark Master | http://localhost:8083 | - |

---

## Stop the Pipeline

### Stop Python Scripts
Press `Ctrl+C` in each terminal

### Stop Docker Services
```powershell
docker-compose down
```

### Stop and Remove All Data
```powershell
docker-compose down -v
```

---

## Reset Commands (Clean Start)

### Reset MongoDB Database
```powershell
docker exec fraud-mongodb mongosh fraud_detection --eval "db.dropDatabase()"
```

### Reset Kafka Topic
```powershell
docker exec fraud-kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic transactions
docker exec fraud-kafka kafka-topics --bootstrap-server localhost:9092 --create --topic transactions --partitions 6 --replication-factor 1
```

### Full Reset (Database + Kafka)
```powershell
docker exec fraud-mongodb mongosh fraud_detection --eval "db.dropDatabase()"
docker exec fraud-kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic transactions
docker exec fraud-kafka kafka-topics --bootstrap-server localhost:9092 --create --topic transactions --partitions 6 --replication-factor 1
```

---

## Run Spark Batch Job (Optional)

```powershell
docker exec fraud-spark-master spark-submit --master local[*] /app/src/batch/spark_job.py
```

---

## Troubleshooting

### Port Already in Use
```powershell
# Check what's using a port (e.g., 27017)
netstat -ano | findstr :27017

# Kill process by PID
taskkill /PID <PID> /F
```

### Docker Services Not Starting
```powershell
# Restart Docker Desktop, then:
docker-compose down
docker-compose up -d
```

### Kafka Connection Error
```powershell
# Wait for Kafka to be ready
docker logs fraud-kafka

# Check if topic exists
docker exec fraud-kafka kafka-topics --bootstrap-server localhost:9092 --list
```

### MongoDB Connection Error
```powershell
# Check if MongoDB is running
docker logs fraud-mongodb

# Test connection
docker exec fraud-mongodb mongosh --eval "db.adminCommand('ping')"
```

---

## Quick Demo (All Commands)

```powershell
# 1. Start infrastructure
docker-compose up -d

# 2. Wait 30 seconds
Start-Sleep -Seconds 30

# 3. Create topic (if first time)
docker exec fraud-kafka kafka-topics --bootstrap-server localhost:9092 --create --topic transactions --partitions 6 --replication-factor 1 2>$null

# 4. Open new terminals and run:
#    Terminal 1: python src/producer/transaction_producer.py
#    Terminal 2: python src/speed/simple_processor.py
#    Terminal 3: python src/dashboard/pro_dashboard.py

# 5. Open browser: http://localhost:8050
```
