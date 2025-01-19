import time
import logging
from src.service import run_service


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def run_script():
    """Runs the main service script."""
    while True:
        try:
            logging.info("Starting service.py...")
            run_service()
        except Exception as e:
            logging.warning(f"Service.py finished with an error: {e}. Restarting...")
            time.sleep(300)

if __name__ == "__main__":
    run_script()