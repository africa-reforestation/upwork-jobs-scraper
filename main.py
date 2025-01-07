import time
import logging
from src.service import run_service

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def run_script():
    """Runs the main service script."""
    while True:
        logging.info("Starting service.py...")
        run_service()
        logging.warning("Service.py finished. Restarting...")
        time.sleep(1)  # Optional delay before restarting

if __name__ == "__main__":
    run_script()
