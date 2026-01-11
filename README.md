fraud-detection-pipeline/
├── docs/
│   ├── ARCHITECTURE.md          # Architecture design & justification
│   ├── REPORT_OUTLINE.md        # Final report structure
│   └── ORAL_DEFENSE.md          # Defense talking points
├── setup/
│   ├── 01_prerequisites.md      # Java, Python, Spark setup
│   ├── 02_kubernetes_setup.md   # Minikube/Kind setup
│   └── 03_services_setup.md     # Kafka, MongoDB, HDFS setup
├── kubernetes/
│   ├── namespace.yaml
│   ├── kafka/
│   │   ├── zookeeper.yaml
│   │   └── kafka.yaml
│   ├── mongodb/
│   │   └── mongodb.yaml
│   ├── hdfs/
│   │   └── hdfs.yaml
│   └── spark/
│       ├── spark-batch-job.yaml
│       └── spark-stream-job.yaml
├── src/
│   ├── producer/
│   │   └── transaction_producer.py
│   ├── batch/
│   │   └── fraud_detection_batch.py      # BATCH LAYER
│   ├── speed/
│   │   └── fraud_detection_stream.py     # SPEED LAYER
│   ├── serving/
│   │   ├── serving_layer.py              # SERVING LAYER
│   │   └── query_api.py                  # Query interface
│   └── ml/
│       ├── train_model.py                # Batch model training
│       └── fraud_model.py                # Model utilities
├── tests/
│   ├── test_fraud_logic.py
│   ├── test_data_quality.py
│   └── test_batch_speed_consistency.py
├── data/
│   ├── raw/                              # Raw transaction data (HDFS)
│   ├── master/                           # Master dataset (batch views)
│   └── static/
│       └── risk_data.json                # Static risk lookup data
├── config/
│   └── app_config.py
└── requirements.txt
