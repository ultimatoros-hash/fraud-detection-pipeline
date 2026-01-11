# Kubernetes Setup Guide

## 1. Start Minikube Cluster

```powershell
minikube delete  # Clean start
minikube start --memory=8192 --cpus=4 --disk-size=50g --driver=docker

# Enable addons
minikube addons enable ingress
minikube addons enable metrics-server

# Verify
minikube status
kubectl cluster-info
```

---

## 2. Create Namespace

```powershell
kubectl apply -f kubernetes/namespace.yaml
kubectl config set-context --current --namespace=fraud-detection
```

---

## 3. Deploy Infrastructure (In Order)

```powershell
# 1. Namespace & Storage
kubectl apply -f kubernetes/namespace.yaml
kubectl apply -f kubernetes/hdfs/

# 2. Zookeeper → Kafka
kubectl apply -f kubernetes/kafka/zookeeper.yaml
kubectl wait --for=condition=ready pod -l app=zookeeper --timeout=120s
kubectl apply -f kubernetes/kafka/kafka.yaml
kubectl wait --for=condition=ready pod -l app=kafka --timeout=120s

# 3. MongoDB
kubectl apply -f kubernetes/mongodb/
kubectl wait --for=condition=ready pod -l app=mongodb --timeout=120s

# 4. Monitoring (Prometheus + Grafana)
kubectl apply -f kubernetes/monitoring/
kubectl wait --for=condition=ready pod -l app=prometheus --timeout=120s

# 5. Dashboard
kubectl apply -f kubernetes/dashboard/
```

---

## 4. Port Forwarding

```powershell
# Run each in separate terminal or as background jobs
kubectl port-forward svc/kafka 9092:9092 -n fraud-detection
kubectl port-forward svc/mongodb 27017:27017 -n fraud-detection
kubectl port-forward svc/prometheus 9090:9090 -n fraud-detection
kubectl port-forward svc/grafana 3000:3000 -n fraud-detection
kubectl port-forward svc/dash-dashboard 8050:8050 -n fraud-detection
```

---

## 5. Verify Deployment

```powershell
kubectl get pods -n fraud-detection
kubectl get services -n fraud-detection
kubectl get pvc -n fraud-detection
```

---

## Service Endpoints

| Service | Port | URL |
|---------|------|-----|
| Kafka | 9092 | localhost:9092 |
| MongoDB | 27017 | localhost:27017 |
| Prometheus | 9090 | http://localhost:9090 |
| Grafana | 3000 | http://localhost:3000 (admin/admin) |
| Dash | 8050 | http://localhost:8050 |

---

## Next Steps
→ [03_services_setup.md](03_services_setup.md)
