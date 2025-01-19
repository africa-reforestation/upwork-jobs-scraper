import time
import logging
from src.service import run_service
import asyncio


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

async def run_script():
    """Runs the main service script."""
    while True:
        logging.info("Starting service.py...")
        await run_service()
        logging.warning("Service.py finished. Restarting...")
        time.sleep(1) 

if __name__ == "__main__":
    asyncio.run(run_script())