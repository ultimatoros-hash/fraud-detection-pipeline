import subprocess
import time
import sys
import os

class Colors:
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    END = '\033[0m'

def print_banner():
    print(f"""
{Colors.CYAN}{Colors.BOLD}

           FRAUD DETECTION PIPELINE - DEMO                    
                  Lambda Architecture                         

{Colors.END}""")

def print_step(num, msg):
    print(f"{Colors.YELLOW}[{num}]{Colors.END} {msg}")

def print_success(msg):
    print(f"    {Colors.GREEN}{Colors.END} {msg}")

def print_error(msg):
    print(f"    {Colors.RED}{Colors.END} {msg}")

def run_command(cmd, check=True):
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=30)
        return result.returncode == 0
    except:
        return False

def check_docker():
    containers = ['fraud-kafka', 'fraud-mongodb', 'fraud-zookeeper']
    all_running = True
    for container in containers:
        result = subprocess.run(f'docker ps --filter "name={container}" --format "{{{{.Names}}}}"', 
                              shell=True, capture_output=True, text=True)
        if container not in result.stdout:
            all_running = False
            print_error(f"{container} not running")
        else:
            print_success(f"{container} running")
    return all_running

def clear_mongodb():
    try:
        from pymongo import MongoClient
        client = MongoClient('mongodb://localhost:27017', serverSelectionTimeoutMS=5000)
        db = client['fraud_detection']
        
        db['realtime_views'].drop()
        db['fraud_alerts'].drop()
        db['batch_views'].drop()
        
        print_success("MongoDB collections cleared")
        return True
    except Exception as e:
        print_error(f"MongoDB error: {e}")
        return False

def stop_python_processes():
    try:
        if sys.platform == 'win32':
            os.system('taskkill /F /IM python.exe 2>nul')
        else:
            os.system('pkill -f python 2>/dev/null')
        time.sleep(1)
        print_success("Done")
        return True
    except Exception as e:
        print_success("Done")
        return True

def main():
    print_banner()
    
    print_step(1, "Checking Docker services...")
    if not check_docker():
        print(f"\n{Colors.RED}Docker containers not running!{Colors.END}")
        print(f"Run: {Colors.CYAN}docker-compose up -d{Colors.END}")
        sys.exit(1)
    
    print_step(2, "Stopping existing Python processes...")
    stop_python_processes()
    print_success("Done")
    
    print_step(3, "Clearing MongoDB for fresh demo...")
    clear_mongodb()
    
    print(f"""
{Colors.CYAN}{Colors.BOLD}

  READY TO LAUNCH!
{Colors.END}

Open 3 separate terminals and run these commands:

{Colors.GREEN}Terminal 1 - Producer:{Colors.END}
  python src/producer/transaction_producer.py

{Colors.GREEN}Terminal 2 - Speed Layer (Processor):{Colors.END}
  python src/speed/simple_processor.py

{Colors.GREEN}Terminal 3 - Dashboard:{Colors.END}
  python src/dashboard/pro_dashboard.py

{Colors.CYAN}Then open: http://localhost:8050{Colors.END}

{Colors.YELLOW}Other useful URLs:{Colors.END}
   Kafka UI:      http://localhost:8082
   Mongo Express: http://localhost:8081
   Grafana:       http://localhost:3000
   Prometheus:    http://localhost:9090

{Colors.CYAN}To run Spark batch job:{Colors.END}
  docker exec fraud-spark-master spark-submit --master local[*] /app/src/batch/spark_job.py
""")

if __name__ == "__main__":
    main()
