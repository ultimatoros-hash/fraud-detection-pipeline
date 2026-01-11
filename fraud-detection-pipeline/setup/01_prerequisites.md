# Prerequisites Setup Guide

---
## 1. Java Development Kit (JDK 11+)

### Windows (PowerShell as Administrator)
```powershell
# Option 1: Using Chocolatey
choco install openjdk11 -y

# Option 2: Manual Download from https://adoptium.net/temurin/releases/
# Install and set environment variables:
[System.Environment]::SetEnvironmentVariable("JAVA_HOME", "C:\Program Files\Eclipse Adoptium\jdk-11.0.21.9-hotspot", "Machine")
$env:Path += ";$env:JAVA_HOME\bin"

# Verify installation
java -version
```

---

## 2. Python 3.9+

```powershell
# Using Chocolatey
choco install python --version=3.11.0 -y

# Verify
python --version
pip --version

# Create virtual environment
python -m venv fraud-detection-env
.\fraud-detection-env\Scripts\Activate.ps1
pip install -r requirements.txt
```

---

## 3. Apache Spark 3.5.0

```powershell
# Download and extract Spark
Invoke-WebRequest -Uri "https://dlcdn.apache.org/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz" 
tar -xzf spark-3.5.0.tgz
Move-Item spark-3.5.0-bin-hadoop3 "C:\Spark"

# Set environment variables
[System.Environment]::SetEnvironmentVariable("SPARK_HOME", "C:\Spark", "Machine")
$env:Path += ";$env:SPARK_HOME\bin"

# For Windows: Download winutils from https://github.com/steveloughran/winutils
[System.Environment]::SetEnvironmentVariable("HADOOP_HOME", "C:\hadoop", "Machine")

# Verify
spark-submit --version
```

---

## 4. Docker Desktop & Kubernetes

```powershell
# Install Docker Desktop
choco install docker-desktop -y

# Install kubectl
choco install kubernetes-cli -y

# Install Minikube
choco install minikube -y

# Install Helm
choco install kubernetes-helm -y

# Verify
docker --version
kubectl version --client
minikube version
helm version
```

---

## 5. Python Dependencies (requirements.txt)

```
# Core
pyspark==3.5.0
pandas>=2.0.0
numpy>=1.24.0
pyarrow>=14.0.0

# Kafka
kafka-python>=2.0.2

# MongoDB
pymongo>=4.6.0

# ML
scikit-learn>=1.3.0

# Dashboard
dash>=2.14.0
dash-bootstrap-components>=1.5.0
plotly>=5.18.0

# Data Generation
faker>=22.0.0

# Monitoring
prometheus-client>=0.19.0

# Testing
pytest>=7.4.0
```

Install with: `pip install -r requirements.txt`

---

## 6. Create Project Structure

```powershell
$dirs = @(
    "src\producer", "src\batch", "src\speed", "src\serving", "src\ml", "src\dashboard",
    "kubernetes\kafka", "kubernetes\mongodb", "kubernetes\hdfs", "kubernetes\spark", 
    "kubernetes\monitoring", "kubernetes\dashboard",
    "data\raw\transactions", "data\master", "data\static",
    "checkpoints\batch-layer", "checkpoints\speed-layer",
    "models", "tests", "grafana\dashboards", "grafana\provisioning"
)
foreach ($d in $dirs) { 
    New-Item -ItemType Directory -Force -Path "c:\Projects\fraud-detection-pipeline\$d" 
}
```

---

## Next Steps
→ [02_kubernetes_setup.md](02_kubernetes_setup.md)
