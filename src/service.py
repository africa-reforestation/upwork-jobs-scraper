import time
import logging
from .utils import scrape_upwork_data

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def run_service():
    logging.info("Started scraping Upwork data...")
    scrape_upwork_data("langgraph ai jobs", num_jobs=50)

if __name__ == "__main__":
    run_service()
