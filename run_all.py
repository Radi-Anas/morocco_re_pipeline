"""
run_all.py
Run API and Dashboard together.

Usage:
    python run_all.py
"""

import subprocess
import sys
import os

def main():
    processes = []
    
    print("Starting Morocco RE Pipeline Services...")
    print("=" * 50)
    
    # Start API
    print("Starting FastAPI on http://localhost:8000")
    api_proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "api:app", "--reload", "--port", "8000"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    processes.append(("API", api_proc))
    
    # Start Dashboard
    print("Starting Streamlit on http://localhost:8501")
    dash_proc = subprocess.Popen(
        [sys.executable, "-m", "streamlit", "run", "dashboard.py", "--server.port", "8501"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    processes.append(("Dashboard", dash_proc))
    
    print("=" * 50)
    print("All services started!")
    print("  API:       http://localhost:8000")
    print("  API Docs:  http://localhost:8000/docs")
    print("  Dashboard: http://localhost:8501")
    print()
    print("Press Ctrl+C to stop all services")
    print("=" * 50)
    
    try:
        for name, proc in processes:
            proc.wait()
    except KeyboardInterrupt:
        print("\nStopping services...")
        for name, proc in processes:
            proc.terminate()
        print("All services stopped.")

if __name__ == "__main__":
    main()
