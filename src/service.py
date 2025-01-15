import subprocess
import time
import logging
import streamlit as st
from .utils import scrape_upwork_data
from prisma import Prisma

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def generate_prisma_client():
    print("GENERATING PRISMA CLIENT")
    subprocess.call(["prisma", "generate"])
    print("GENERATED PRISMA CLIENT")

@st.cache_resource
def init_connection() -> Prisma:
    db = Prisma()
    db.connect()
    return db

def run_service():
    logging.info("Started scraping Upwork data...")
    generate_prisma_client()
    scrape_upwork_data("langgraph ai jobs", num_jobs=50)

if __name__ == "__main__":
    run_service()
