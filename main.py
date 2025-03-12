import argparse
import subprocess
import sys
import threading
import time
import os
import signal
from database import initialize_database
from data_pipeline import update_stock_data
from data_pipeline import run_analysis_and_execute_trade

producer_process = None
consumer_process = None

def main():
    global producer_process, consumer_process

    parser = argparse.ArgumentParser(description="Trading Dashboard Controller")

    parser.add_argument(
        "command",
        choices=["init", "update", "analyze", "produce", "consume", "stop", "restart", "show"],
        help="""Available commands:
        init     → Initialize the SQLite database
        update   → Fetch latest stock, macroeconomic data, news, and sentiment
        analyze  → Run AI-powered analysis and execute a trade
        produce  → Start Kafka Producer for real-time price streaming
        consume  → Start Kafka Consumer for real-time visualization
        stop     → Stop both Producer and Consumer
        restart  → Restart both Producer and Consumer
        show     → Launch stock dashboard (via Streamlit)"""
    )
    
    parser.add_argument(
        "--ticker",
        type=str,
        help="Stock ticker for analysis, streaming, or dashboard (e.g., AAPL)"
    )

    args = parser.parse_args()

    # Initialize Database
    if args.command == "init":
        print("Initializing database...")
        initialize_database()
        print("Database initialized successfully.")

    # Update Data
    elif args.command == "update":
        if not args.ticker:
            print("⚠ Please specify a stock ticker using --ticker <TICKER>")
            return
        
        print(f"Fetching latest data for {args.ticker}...")
        update_stock_data(args.ticker)
        print(f"Data updated for {args.ticker}.")

    # Run AI Analysis
    elif args.command == "analyze":
        if not args.ticker:
            print("⚠ Please specify a stock ticker using --ticker <TICKER>")
            return
        
        print(f"Running AI-powered analysis for {args.ticker}...")
        result = run_analysis_and_execute_trade(args.ticker)
        print(result)

    # Start Kafka Producer (Streaming Data)
    elif args.command == "produce":
        if not args.ticker:
            print("⚠ Please specify a stock ticker using --ticker <TICKER>")
            return
        
        if producer_process is not None:
            print("⚠ Producer is already running.")
        else:
            print(f"🚀 Starting Kafka Producer for {args.ticker}...")
            producer_process = subprocess.Popen(["python", "producer.py", args.ticker])
            time.sleep(2)  # 等待 Producer 启动
            print("Kafka Producer started successfully.")

    # Start Kafka Consumer (Real-Time Visualization)
    elif args.command == "consume":
        if not args.ticker:
            print("⚠ Please specify a stock ticker using --ticker <TICKER>")
            return
        
        if consumer_process is not None:
            print("⚠ Consumer is already running.")
        else:
            print(f"📊 Starting Kafka Consumer for {args.ticker}...")
            consumer_process = subprocess.Popen(["python", "consumer.py", args.ticker])
            print("Kafka Consumer started successfully.")

    # Stop Kafka Producer & Consumer
    elif args.command == "stop":
        if producer_process is not None:
            os.kill(producer_process.pid, signal.SIGTERM)
            producer_process = None
            print("🛑 Producer stopped.")
        else:
            print("⚠ No active Producer to stop.")

        if consumer_process is not None:
            os.kill(consumer_process.pid, signal.SIGTERM)
            consumer_process = None
            print("🛑 Consumer stopped.")
        else:
            print("⚠ No active Consumer to stop.")

    # Restart Kafka Producer & Consumer
    elif args.command == "restart":
        if producer_process is not None:
            os.kill(producer_process.pid, signal.SIGTERM)
            producer_process = None
            print("🛑 Producer stopped.")

        if consumer_process is not None:
            os.kill(consumer_process.pid, signal.SIGTERM)
            consumer_process = None
            print("🛑 Consumer stopped.")

        time.sleep(2)  # 等待进程完全停止

        print(f"🚀 Restarting Kafka Producer and Consumer for {args.ticker}...")
        producer_process = subprocess.Popen(["python", "producer.py", args.ticker])
        consumer_process = subprocess.Popen(["python", "consumer.py", args.ticker])
        print("Kafka Producer & Consumer restarted successfully.")

    # Launch Streamlit Dashboard
    elif args.command == "show":
        if not args.ticker:
            print("⚠ Please specify a stock ticker using --ticker <TICKER>")
            return

        print(f"Launching stock dashboard for {args.ticker}...")
        try:
            subprocess.run(["streamlit", "run", "stock_dashboard.py", args.ticker])
        except FileNotFoundError:
            print("Error: Streamlit is not installed or stock_dashboard.py is missing.")

if __name__ == "__main__":
    main()
