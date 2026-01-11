# Architecture Design Document

## 1. Architecture Overview

This project implements **Lambda Architecture** for real-time fraud detection on worldwide financial transactions. Lambda Architecture provides both **real-time responsiveness** (speed layer) and **complete accuracy** (batch layer).

---

## 2. ASCII Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                          LAMBDA ARCHITECTURE - FRAUD DETECTION                           │
└─────────────────────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                                    DATA INGESTION                                         │
│                                                                                          │
│   ┌──────────────┐          ┌─────────────────────────────────────────────────────┐     │
│   │ Transaction  │          │                   Apache Kafka                       │     │
│   │ Sources      │          │                                                      │     │
│   │              │          │  ┌─────────────────────────────────────────────┐    │     │
│   │ • Banks      ├─────────►│  │  Topic: transactions                        │    │     │
│   │ • Payment    │          │  │  Partitions: 6    Replication: 3            │    │     │
│   │   Gateways   │          │  │  Retention: 7 days                          │    │     │
│   │ • Mobile     │          │  └─────────────────────────────────────────────┘    │     │
│   │   Apps       │          │                       │                              │     │
│   └──────────────┘          └───────────────────────┼──────────────────────────────┘     │
│                                                     │                                     │
└─────────────────────────────────────────────────────┼─────────────────────────────────────┘
                                                      │
                          ┌───────────────────────────┴───────────────────────────┐
                          │                                                       │
                          ▼                                                       ▼
┌─────────────────────────────────────────────┐     ┌─────────────────────────────────────────────┐
│              BATCH LAYER                    │     │              SPEED LAYER                    │
│         (Accuracy & Completeness)           │     │            (Low Latency)                    │
├─────────────────────────────────────────────┤     ├─────────────────────────────────────────────┤
│                                             │     │                                             │
│  ┌───────────────────────────────────────┐  │     │  ┌───────────────────────────────────────┐  │
│  │        HDFS / Master Dataset          │  │     │  │      Spark Structured Streaming      │  │
│  │                                       │  │     │  │                                       │  │
│  │  • Raw transactions (immutable)       │  │     │  │  • Watermarking (30 seconds)         │  │
│  │  • Append-only log                    │  │     │  │  • Windowing (1 minute tumbling)     │  │
│  │  • Partitioned by date                │  │     │  │  • Stateful deduplication            │  │
│  └───────────────────────────────────────┘  │     │  │  • Real-time fraud scoring           │  │
│                     │                       │     │  │  • Micro-batch: 5 seconds            │  │
│                     ▼                       │     │  └───────────────────────────────────────┘  │
│  ┌───────────────────────────────────────┐  │     │                     │                       │
│  │         Spark Batch Job               │  │     │                     │                       │
│  │                                       │  │     │                     ▼                       │
│  │  • Historical pattern analysis        │  │     │  ┌───────────────────────────────────────┐  │
│  │  • ML model training (MLlib)          │  │     │  │        Real-time Views               │  │
│  │  • Complete aggregations              │  │     │  │                                       │  │
│  │  • User risk profiling                │  │     │  │  • Recent fraud alerts (< 1 hour)    │  │
│  │  • Scheduled: Every 1 hour            │  │     │  │  • Current transaction stats         │  │
│  └───────────────────────────────────────┘  │     │  │  • Live risk scores                  │  │
│                     │                       │     │  └───────────────────────────────────────┘  │
│                     ▼                       │     │                                             │
│  ┌───────────────────────────────────────┐  │     └─────────────────────────────────────────────┘
│  │          Batch Views                  │  │                           │
│  │                                       │  │                           │
│  │  • Complete fraud history             │  │                           │
│  │  • Accurate user profiles             │  │                           │
│  │  • Trained ML models                  │  │                           │
│  │  • Historical aggregations            │  │                           │
│  └───────────────────────────────────────┘  │                           │
│                     │                       │                           │
└─────────────────────┼───────────────────────┘                           │
                      │                                                   │
                      └───────────────────────┬───────────────────────────┘
                                              │
                                              ▼
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                   SERVING LAYER                                          │
│                              (View Merging & Query)                                      │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                          │
│  ┌───────────────────────────────────────────────────────────────────────────────────┐  │
│  │                              MongoDB Cluster                                       │  │
│  │                                                                                    │  │
│  │  ┌─────────────────────────┐  ┌─────────────────────────┐  ┌───────────────────┐  │  │
│  │  │    batch_views          │  │   realtime_views        │  │   merged_views    │  │  │
│  │  │                         │  │                         │  │                   │  │  │
│  │  │  • fraud_history        │  │  • recent_alerts        │  │  Query combines   │  │  │
│  │  │  • user_profiles        │  │  • live_stats           │  │  batch + realtime │  │  │
│  │  │  • trained_models       │  │  • current_scores       │  │  results          │  │  │
│  │  │  (updated hourly)       │  │  (updated real-time)    │  │                   │  │  │
│  │  └─────────────────────────┘  └─────────────────────────┘  └───────────────────┘  │  │
│  │                                                                                    │  │
│  └───────────────────────────────────────────────────────────────────────────────────┘  │
│                                              │                                           │
│                                              ▼                                           │
│  ┌───────────────────────────────────────────────────────────────────────────────────┐  │
│  │                            Query API / Dashboard                                   │  │
│  │                                                                                    │  │
│  │  query(user_id, time_range):                                                      │  │
│  │      batch_result = batch_views.find(user_id, time < last_batch_run)              │  │
│  │      realtime_result = realtime_views.find(user_id, time >= last_batch_run)       │  │
│  │      return merge(batch_result, realtime_result)                                  │  │
│  │                                                                                    │  │
│  └───────────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                          │
└─────────────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                              KUBERNETES CLUSTER (Minikube)                               │
│                                                                                          │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐    │
│  │  Zookeeper  │  │   Kafka     │  │  HDFS       │  │   Spark     │  │  MongoDB    │    │
│  │  Pod        │  │   Pod       │  │  NameNode   │  │  Driver +   │  │  ReplicaSet │    │
│  │             │  │             │  │  DataNode   │  │  Executors  │  │             │    │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘    │
│                                                                                          │
└─────────────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                              STATIC DATA (Broadcast)                                     │
│                                                                                          │
│  ┌───────────────────────────────────────────────────────────────────────────────────┐  │
│  │  • Country Risk Scores (US: 0.1, NG: 0.9, ...)                                    │  │
│  │  • Merchant Risk Scores (grocery: 0.1, gambling: 0.9, ...)                        │  │
│  │  • User Risk Profiles (from batch layer)                                          │  │
│  │  • Blacklisted Merchants                                                          │  │
│  └───────────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                          │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Lambda vs Kappa Architecture Justification

### Why Lambda Architecture for Fraud Detection?

| Aspect | Lambda | Kappa | Our Choice |
|--------|--------|-------|------------|
| **Accuracy** | Complete (batch recomputes all) | May drift over time | ✅ Lambda |
| **Latency** | Real-time via speed layer | Real-time only | ✅ Lambda |
| **ML Training** | Batch layer trains on all data | Complex in stream | ✅ Lambda |
| **Historical Analysis** | Native batch processing | Requires replay | ✅ Lambda |
| **Complexity** | Higher (2 codebases) | Lower (1 codebase) | ⚠️ Kappa simpler |
| **Maintenance** | More effort | Less effort | ⚠️ Kappa easier |
| **Reprocessing** | Batch layer handles | Kafka replay | Both work |

### Specific Justification for Fraud Detection:

#### 1. **Accuracy Requirements**
- Financial fraud detection MUST be accurate - false positives block legitimate transactions
- Batch layer can recompute ALL history with latest algorithms
- Speed layer provides approximate real-time alerts

#### 2. **ML Model Training**
- Training fraud models requires COMPLETE historical data
- Batch layer periodically retrains models on full dataset
- Speed layer uses pre-trained models for inference

#### 3. **Regulatory Compliance**
- Financial regulations require historical audit trails
- Batch layer maintains complete, immutable transaction history
- Speed layer provides immediate alerts

#### 4. **Pattern Detection**
- Complex fraud rings require cross-user analysis
- Batch processing can detect multi-day fraud patterns
- Speed layer catches immediate anomalies

#### 5. **Trade-off Acceptance**
- We accept higher complexity for better fraud detection
- The cost of missed fraud > cost of system complexity
- Two codebases share common logic via shared modules

### When Kappa Would Be Preferred:
- Simple alerting without ML training needs
- If Kafka retention could hold ALL historical data
- When team size limits maintenance capacity
- For simpler use cases like log aggregation

---

## 4. Layer Responsibilities

### Batch Layer
```
┌────────────────────────────────────────────────────────────────────┐
│                         BATCH LAYER                                 │
├────────────────────────────────────────────────────────────────────┤
│                                                                    │
│  INPUT:                                                            │
│    • Master dataset (all historical transactions from HDFS)        │
│    • Static reference data (risk scores, blacklists)               │
│                                                                    │
│  PROCESSING:                                                       │
│    • Scheduled execution (every 1 hour)                            │
│    • Complete recomputation of views                               │
│    • ML model training with full dataset                           │
│    • Complex aggregations (user profiles, fraud patterns)          │
│                                                                    │
│  OUTPUT (Batch Views):                                             │
│    • fraud_history: Complete fraud detection results               │
│    • user_profiles: Historical user behavior patterns              │
│    • ml_models: Trained fraud detection models                     │
│    • aggregations: Daily/weekly/monthly statistics                 │
│                                                                    │
│  CHARACTERISTICS:                                                  │
│    • High latency (minutes to hours)                               │
│    • High accuracy (complete data)                                 │
│    • Idempotent (same input = same output)                        │
│    • Scalable (add more executors)                                 │
│                                                                    │
└────────────────────────────────────────────────────────────────────┘
```

### Speed Layer
```
┌────────────────────────────────────────────────────────────────────┐
│                         SPEED LAYER                                 │
├────────────────────────────────────────────────────────────────────┤
│                                                                    │
│  INPUT:                                                            │
│    • Real-time Kafka stream (recent transactions only)             │
│    • Pre-trained ML models (from batch layer)                      │
│    • Static reference data (broadcasted)                           │
│                                                                    │
│  PROCESSING:                                                       │
│    • Continuous streaming (5-second micro-batches)                 │
│    • Incremental aggregations only                                 │
│    • Real-time fraud scoring                                       │
│    • Watermarking for late event handling                          │
│                                                                    │
│  OUTPUT (Real-time Views):                                         │
│    • recent_alerts: Fraud alerts from last hour                    │
│    • live_stats: Current transaction statistics                    │
│    • current_scores: Real-time user risk scores                    │
│                                                                    │
│  CHARACTERISTICS:                                                  │
│    • Low latency (seconds)                                         │
│    • Approximate accuracy (recent data only)                       │
│    • Handles late events via watermarking                          │
│    • Compensates for batch layer latency                           │
│                                                                    │
└────────────────────────────────────────────────────────────────────┘
```

### Serving Layer
```
┌────────────────────────────────────────────────────────────────────┐
│                        SERVING LAYER                                │
├────────────────────────────────────────────────────────────────────┤
│                                                                    │
│  FUNCTION: Merge batch views + real-time views                     │
│                                                                    │
│  MERGE STRATEGY:                                                   │
│    • For time T < last_batch_run: Use batch views                  │
│    • For time T >= last_batch_run: Use real-time views             │
│    • Combine results for complete picture                          │
│                                                                    │
│  QUERY EXAMPLE:                                                    │
│    get_user_fraud_alerts(user_id, start_time, end_time):           │
│        last_batch = get_last_batch_timestamp()                     │
│        batch_alerts = batch_views.find(                            │
│            user_id, time >= start_time, time < last_batch          │
│        )                                                           │
│        realtime_alerts = realtime_views.find(                      │
│            user_id, time >= last_batch, time <= end_time           │
│        )                                                           │
│        return batch_alerts + realtime_alerts                       │
│                                                                    │
│  WHY MONGODB:                                                      │
│    • Flexible schema for different view types                      │
│    • Rich query language for merging                               │
│    • TTL indexes for automatic cleanup                             │
│    • Horizontal scaling via sharding                               │
│                                                                    │
└────────────────────────────────────────────────────────────────────┘
```

---

## 5. Scalability Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     SCALABILITY DIMENSIONS                           │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  1. KAFKA SCALABILITY                                               │
│     ├── Horizontal: Add more brokers (3 → 6 → 12)                   │
│     ├── Partition scaling: 6 → 12 → 24 partitions                   │
│     ├── Consumer parallelism: 1 consumer per partition              │
│     └── Throughput: ~100K messages/sec per broker                   │
│                                                                     │
│  2. BATCH LAYER SCALABILITY                                         │
│     ├── Executor scaling: 2 → 10 → 50 executors                     │
│     ├── Data partitioning: By date, user_id hash                    │
│     ├── HDFS scaling: Add DataNodes                                 │
│     └── Processing: Linear scaling with executors                   │
│                                                                     │
│  3. SPEED LAYER SCALABILITY                                         │
│     ├── Kafka partitions = Spark parallelism                        │
│     ├── Micro-batch tuning: 5s → 1s for higher throughput           │
│     ├── Checkpoint optimization: Async checkpointing                │
│     └── State management: RocksDB for large state                   │
│                                                                     │
│  4. SERVING LAYER SCALABILITY                                       │
│     ├── MongoDB sharding: By user_id                                │
│     ├── Read replicas: For query scaling                            │
│     ├── Index optimization: Compound indexes                        │
│     └── Caching: Redis for hot queries                              │
│                                                                     │
│  5. KUBERNETES SCALABILITY                                          │
│     ├── HPA: Auto-scale pods based on CPU/memory                    │
│     ├── Node pools: Add worker nodes                                │
│     ├── Resource quotas: Namespace isolation                        │
│     └── Pod disruption budgets: Controlled upgrades                 │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 6. Fault Tolerance Mechanisms

### Kafka Fault Tolerance
- **Replication Factor = 3**: Data survives 2 broker failures
- **ISR (In-Sync Replicas)**: Only synced replicas serve reads
- **Acks = all**: Producer waits for all replicas to confirm
- **Unclean Leader Election = false**: Prevent data loss

### Batch Layer Fault Tolerance
- **HDFS Replication**: 3 copies of each block
- **Spark Checkpointing**: State recovery on failure
- **Idempotent Processing**: Re-run produces same results
- **Lineage-based Recovery**: Recompute lost partitions

### Speed Layer Fault Tolerance
- **Checkpointing**: Every micro-batch state saved
- **Write-Ahead Logs**: Ensures exactly-once processing
- **Kafka Offset Tracking**: Resume from last committed offset
- **Watermarking**: Handle late events gracefully

### Serving Layer Fault Tolerance
- **MongoDB Replica Sets**: Automatic failover
- **Write Concern = majority**: Durable writes
- **Read Preference = primaryPreferred**: Read availability
- **Connection Pooling**: Handle connection failures

---

## 7. Replayability

```
REPLAYABILITY IN LAMBDA ARCHITECTURE:
─────────────────────────────────────

┌─────────────────────────────────────────────────────────────────────┐
│                    BATCH LAYER REPLAYABILITY                         │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  MASTER DATASET (HDFS):                                             │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │  /data/raw/transactions/                                     │   │
│  │  ├── year=2025/month=01/day=10/  ← Immutable partitions     │   │
│  │  ├── year=2025/month=01/day=11/                              │   │
│  │  └── ...                                                     │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                                                                     │
│  REPROCESSING:                                                      │
│  1. Deploy new batch job version                                    │
│  2. Run against entire master dataset                               │
│  3. Generate new batch views                                        │
│  4. Atomic swap of old → new views                                  │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                    SPEED LAYER REPLAYABILITY                         │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  KAFKA RETENTION (7 days):                                          │
│  1. Reset consumer offset to desired timestamp                      │
│  2. Clear real-time views                                           │
│  3. Reprocess from Kafka                                            │
│  4. Views automatically rebuilt                                     │
│                                                                     │
│  LIMITATION: Only 7 days of replay capability                       │
│  MITIGATION: Batch layer handles older data                         │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 8. Exactly-Once Semantics

### Batch Layer
- **Idempotent by Design**: Same input always produces same output
- **Atomic View Updates**: Replace views atomically
- **No Duplicates**: Full recomputation eliminates duplicates

### Speed Layer
```python
# Exactly-once configuration in Spark Structured Streaming
stream.writeStream \
    .format("mongodb") \
    .option("checkpointLocation", "/checkpoints/speed-layer") \
    .option("forEachBatch.mode", "append") \
    .outputMode("update") \
    .start()

# Kafka consumer configuration
.option("kafka.enable.idempotence", "true") \
.option("kafka.isolation.level", "read_committed")
```

### End-to-End Guarantee
1. **Producer → Kafka**: Idempotent producer (exactly-once)
2. **Kafka → Spark**: Checkpointed offsets (exactly-once)
3. **Spark → MongoDB**: Transactional writes (exactly-once)

---

## 9. Data Quality Handling

```
┌─────────────────────────────────────────────────────────────────────┐
│                      DATA QUALITY STRATEGY                           │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  LATE EVENTS:                                                       │
│  ├── Speed Layer: Watermark (30 sec), drop if > 5 min late          │
│  └── Batch Layer: Process ALL events regardless of lateness         │
│                                                                     │
│  DUPLICATE EVENTS:                                                  │
│  ├── Speed Layer: Stateful dedup using transaction_id               │
│  └── Batch Layer: DISTINCT on transaction_id                        │
│                                                                     │
│  MALFORMED DATA:                                                    │
│  ├── Schema validation at ingestion                                 │
│  ├── Dead letter queue for invalid records                          │
│  └── Metrics on rejection rate                                      │
│                                                                     │
│  MISSING FIELDS:                                                    │
│  ├── Default values for optional fields                             │
│  ├── Reject records missing required fields                         │
│  └── Imputation in batch layer for analytics                        │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 10. Why NoSQL (MongoDB) for Serving Layer?

| Requirement | MongoDB Capability |
|-------------|-------------------|
| Flexible schema | Different view types (batch/realtime) |
| Fast reads | Indexed queries, sharding |
| Time-series data | TTL indexes for automatic cleanup |
| Horizontal scaling | Sharding by user_id |
| Rich queries | Aggregation pipeline for merging |
| Operational simplicity | Managed cloud options |

### MongoDB Collections Schema

```javascript
// batch_views.fraud_history
{
  "_id": ObjectId,
  "transaction_id": "uuid",
  "user_id": "USER_00001",
  "fraud_score": 0.85,
  "is_fraud": true,
  "amount": 5000.00,
  "timestamp": ISODate("2025-01-10T10:30:00Z"),
  "batch_timestamp": ISODate("2025-01-10T12:00:00Z"),  // When batch ran
  "features": {
    "amount_zscore": 3.2,
    "country_risk": 0.9,
    "velocity_score": 0.7
  }
}

// realtime_views.recent_alerts
{
  "_id": ObjectId,
  "transaction_id": "uuid",
  "user_id": "USER_00001",
  "fraud_score": 0.82,
  "is_fraud": true,
  "amount": 4500.00,
  "timestamp": ISODate("2025-01-10T14:45:00Z"),
  "processing_time": ISODate("2025-01-10T14:45:02Z"),
  "ttl": ISODate("2025-01-10T15:45:00Z")  // Auto-delete after 1 hour
}
```

---

## 11. Performance Optimization Strategies

### Batch Layer Optimization
```python
# Partition tuning for batch processing
spark.conf.set("spark.sql.shuffle.partitions", "200")

# Caching intermediate results
user_profiles = calculate_user_profiles(transactions).cache()

# Broadcast small tables
country_risk_bc = spark.sparkContext.broadcast(country_risk_dict)
```

### Speed Layer Optimization
```python
# Micro-batch tuning
.trigger(processingTime="5 seconds")

# State store optimization
spark.conf.set("spark.sql.streaming.stateStore.providerClass", 
               "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")

# Backpressure handling
spark.conf.set("spark.streaming.backpressure.enabled", "true")
```

### Serving Layer Optimization
```javascript
// MongoDB indexes
db.fraud_alerts.createIndex({ "user_id": 1, "timestamp": -1 })
db.fraud_alerts.createIndex({ "timestamp": 1 }, { expireAfterSeconds: 3600 })
```
