import time
import logging
from utils.web_scraping import scrape_upwork_data

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

async def run_service():
    logging.info("Started scraping Upwork data...")
    await scrape_upwork_data("langgraph ai jobs", num_jobs=50)

if __name__ == "__main__":
    import asyncio
    asyncio.run(run_service())
