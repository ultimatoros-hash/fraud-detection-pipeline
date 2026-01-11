#!/usr/bin/env python3

import os
import sys
import subprocess
import time
import signal
import argparse
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

def get_python_cmd():
    return sys.executable

def run_component(name: str, script_path: str, background: bool = True):
    print(f" Starting {name}...")
    
    cmd = [get_python_cmd(), str(PROJECT_ROOT / script_path)]
    
    if background:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True
        )
        return process
    else:
        subprocess.run(cmd)
        return None

def run_producer():
    return run_component(
        "Transaction Producer",
        "src/producer/transaction_producer.py"
    )

def run_batch():
    return run_component(
        "Batch Layer",
        "src/batch/fraud_detection_batch.py",
        background=False
    )

def run_stream():
    return run_component(
        "Speed Layer (Streaming)",
        "src/speed/fraud_detection_stream.py"
    )

def run_dashboard():
    return run_component(
        "Dash Dashboard",
        "src/dashboard/app.py"
    )

def run_metrics():
    return run_component(
        "Metrics Exporter",
        "src/monitoring/metrics_exporter.py"
    )

def run_all():
    processes = []
    
    print("=" * 60)
    print("FRAUD DETECTION PIPELINE - LAMBDA ARCHITECTURE")
    print("=" * 60)
    
    proc = run_metrics()
    if proc:
        processes.append(("Metrics", proc))
        time.sleep(2)
    
    proc = run_dashboard()
    if proc:
        processes.append(("Dashboard", proc))
        time.sleep(3)
    
    proc = run_stream()
    if proc:
        processes.append(("Stream", proc))
        time.sleep(5)
    
    proc = run_producer()
    if proc:
        processes.append(("Producer", proc))
    
    print("\n" + "=" * 60)
    print("PIPELINE RUNNING")
    print("=" * 60)
    print(" Dashboard: http://localhost:8050")
    print(" Metrics:   http://localhost:8000/metrics")
    print("\nPress Ctrl+C to stop all components")
    print("=" * 60)
    
    def shutdown(signum, frame):
        print("\n\n Shutting down pipeline...")
        for name, proc in processes:
            print(f"  Stopping {name}...")
            proc.terminate()
            proc.wait(timeout=5)
        print(" Pipeline stopped")
        sys.exit(0)
    
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)
    
    try:
        while True:
            for name, proc in processes:
                if proc.poll() is not None:
                    print(f" {name} exited with code {proc.returncode}")
            time.sleep(5)
    except KeyboardInterrupt:
        shutdown(None, None)

def main():
    parser = argparse.ArgumentParser(description="Run Fraud Detection Pipeline")
    parser.add_argument(
        "component",
        nargs="?",
        default="all",
        choices=["all", "producer", "batch", "stream", "dashboard", "metrics"],
        help="Component to run (default: all)"
    )
    
    args = parser.parse_args()
    
    if args.component == "all":
        run_all()
    elif args.component == "producer":
        run_producer()
    elif args.component == "batch":
        run_batch()
    elif args.component == "stream":
        run_stream()
    elif args.component == "dashboard":
        run_dashboard()
    elif args.component == "metrics":
        run_metrics()

if __name__ == "__main__":
    main()
