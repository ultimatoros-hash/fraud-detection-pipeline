# 🚀 Beginner's Complete Setup Guide
## Fraud Detection Pipeline - Lambda Architecture

This guide walks you through **every single step** to get the project running. No prior experience needed!

---

## 📋 Table of Contents

1. [Prerequisites Installation](#1-prerequisites-installation)
2. [Project Setup](#2-project-setup)
3. [Start Services with Docker](#3-start-services-with-docker)
4. [Create Kafka Topic](#4-create-kafka-topic)
5. [Run the Pipeline](#5-run-the-pipeline)
6. [Access Dashboards](#6-access-dashboards)
7. [Troubleshooting](#7-troubleshooting)

---

## 1. Prerequisites Installation

### 1.1 Install Python 3.9+ ✅

**Check if installed:**
```powershell
python --version
```

**If not installed:**
1. Go to https://www.python.org/downloads/
2. Download Python 3.11 (recommended)
3. Run installer → **CHECK "Add Python to PATH"** ⚠️
4. Click "Install Now"

---

### 1.2 Install Java 11+ ☕

**Check if installed:**
```powershell
java -version
```

**If not installed:**
1. Go to https://adoptium.net/
2. Download "Temurin 11 LTS" or "Temurin 17 LTS"
3. Run installer with default options
4. **Restart your terminal after installation**

**Set JAVA_HOME (if needed):**
```powershell
# Find where Java is installed, then:
[Environment]::SetEnvironmentVariable("JAVA_HOME", "C:\Program Files\Eclipse Adoptium\jdk-17.0.x-hotspot", "User")
```

---

### 1.3 Install Docker Desktop 🐳

**Check if installed:**
```powershell
docker --version
```

**If not installed:**
1. Go to https://www.docker.com/products/docker-desktop/
2. Download Docker Desktop for Windows
3. Run installer
4. **Restart your computer**
5. Open Docker Desktop and wait for it to start (whale icon in system tray)

**Verify Docker is running:**
```powershell
docker ps
```
Should show an empty table (no error).

---

## 2. Project Setup

### 2.1 Open Terminal in Project Folder

```powershell
cd C:\Projects\fraud-detection-pipeline
```

---

### 2.2 Create Virtual Environment (Recommended)

```powershell
# Create virtual environment
python -m venv venv

# Activate it (Windows PowerShell)
.\venv\Scripts\Activate.ps1

# If you get a permission error, run this first:
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

You should see `(venv)` at the start of your prompt.

---

### 2.3 Install Python Dependencies

```powershell
pip install -r requirements.txt
```

**Expected time:** 2-5 minutes (downloads ~350MB for PySpark)

**If you see errors:**
- Make sure you're in the project folder
- Make sure virtual environment is activated `(venv)`

---

## 3. Start Services with Docker

### 3.1 Start All Services

```powershell
# Make sure Docker Desktop is running first!
docker-compose up -d
```

**What this starts:**
| Service | Port | Purpose |
|---------|------|---------|
| Kafka | 9092 | Message broker |
| Zookeeper | 2181 | Kafka coordinator |
| MongoDB | 27017 | Database |
| Prometheus | 9090 | Metrics collection |
| Grafana | 3000 | Monitoring dashboards |
| Kafka UI | 8080 | Kafka web interface |
| Mongo Express | 8081 | MongoDB web interface |

---

### 3.2 Verify Services are Running

```powershell
docker ps
```

You should see 7 containers with status "Up".

**Wait 30 seconds** for all services to fully start, then verify:

```powershell
# Check Kafka is ready
docker exec fraud-kafka kafka-topics --bootstrap-server localhost:9092 --list
```

Should return empty (no topics yet) or show topic names - no error.

---

## 4. Create Kafka Topic

```powershell
docker exec fraud-kafka kafka-topics --bootstrap-server localhost:9092 --create --topic transactions --partitions 6 --replication-factor 1
```

**Expected output:**
```
Created topic transactions.
```

**Verify:**
```powershell
docker exec fraud-kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic transactions
```

---

## 5. Run the Pipeline

### Option A: Run All Components (Easy Way)

```powershell
python scripts/run_pipeline.py
```

This starts everything. Press `Ctrl+C` to stop.

---

### Option B: Run Components Separately (Recommended for Learning)

Open **4 separate PowerShell terminals**, activate venv in each, and run:

**Terminal 1 - Producer (generates transactions):**
```powershell
cd C:\Projects\fraud-detection-pipeline
.\venv\Scripts\Activate.ps1
python src/producer/transaction_producer.py
```

**Terminal 2 - Dashboard (business visualization):**
```powershell
cd C:\Projects\fraud-detection-pipeline
.\venv\Scripts\Activate.ps1
python src/dashboard/app.py
```

**Terminal 3 - Metrics Exporter (Prometheus metrics):**
```powershell
cd C:\Projects\fraud-detection-pipeline
.\venv\Scripts\Activate.ps1
python src/monitoring/metrics_exporter.py
```

**Terminal 4 - Speed Layer (Streaming - optional, needs Spark setup):**
```powershell
cd C:\Projects\fraud-detection-pipeline
.\venv\Scripts\Activate.ps1
python src/speed/fraud_detection_stream.py
```

---

## 6. Access Dashboards

Open these URLs in your browser:

| Dashboard | URL | Login |
|-----------|-----|-------|
| **Dash Dashboard** (Business) | http://localhost:8050 | None |
| **Grafana** (Technical) | http://localhost:3000 | admin / admin123 |
| **Kafka UI** | http://localhost:8080 | None |
| **Mongo Express** | http://localhost:8081 | admin / admin123 |
| **Prometheus** | http://localhost:9090 | None |

---

## 7. Troubleshooting

### ❌ "Docker is not running"

1. Open Docker Desktop from Start Menu
2. Wait for the whale icon to stop animating
3. Try again

---

### ❌ "pip install failed"

```powershell
# Upgrade pip first
python -m pip install --upgrade pip

# Try again
pip install -r requirements.txt
```

---

### ❌ "Java not found" when running Spark

1. Install Java (see section 1.2)
2. **Restart your terminal**
3. Set JAVA_HOME:
```powershell
# Check where Java is
where java

# Set JAVA_HOME (adjust path as needed)
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17.0.9+9"
```

---

### ❌ "Kafka connection refused"

```powershell
# Check if Kafka container is running
docker ps | findstr kafka

# If not running, restart services
docker-compose down
docker-compose up -d

# Wait 30 seconds, then try again
```

---

### ❌ "MongoDB connection failed"

```powershell
# Check if MongoDB container is running
docker ps | findstr mongo

# Test connection
docker exec fraud-mongodb mongosh --eval "db.runCommand('ping')"
```

---

### ❌ "Port already in use"

```powershell
# Find what's using port 8050 (example)
netstat -ano | findstr :8050

# Kill the process (replace PID with actual number)
taskkill /PID <PID> /F
```

---

## 🎉 Quick Reference Commands

```powershell
# ===== STARTUP =====
cd C:\Projects\fraud-detection-pipeline
.\venv\Scripts\Activate.ps1
docker-compose up -d
python scripts/run_pipeline.py

# ===== SHUTDOWN =====
# Press Ctrl+C in each terminal
docker-compose down

# ===== VIEW LOGS =====
docker logs fraud-kafka
docker logs fraud-mongodb

# ===== RESTART EVERYTHING =====
docker-compose down
docker-compose up -d

# ===== CHECK STATUS =====
docker ps
```

---

## 📊 What You Should See

### Dash Dashboard (http://localhost:8050)
- Total transactions counter increasing
- Fraud detection alerts
- Charts showing fraud by country
- Live updating table

### Producer Terminal
```
🚀 Transaction Producer Started
  Kafka: localhost:9092
  Topic: transactions
  Rate: 100 TPS

📤 Produced: txn-abc123... | USER_00042 | $150.00 | US
📤 Produced: txn-def456... | USER_00015 | $2500.00 | NG [FRAUD]
...
```

---

## 🆘 Need Help?

1. Check the error message carefully
2. Google the exact error message
3. Check Docker Desktop is running
4. Make sure all services show "Up" in `docker ps`
5. Restart Docker and try again

---

**You're all set! 🎊**
