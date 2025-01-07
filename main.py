import os
import time
import subprocess

def run_script():
    """Runs the main service script and restarts it if it crashes."""
    while True:
        print("Starting service.py...")
        process = subprocess.Popen(["python", "src/service.py"])
        
        # Wait for the process to finish
        process.wait()
        
        print(f"Service.py exited with code {process.returncode}. Restarting...")
        time.sleep(1)  # Optional delay before restarting

if __name__ == "__main__":
    run_script()
