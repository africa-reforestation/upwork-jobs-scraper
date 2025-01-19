import logging
import random
import re
import streamlit as st
from src.scraper.jobpostcrud import JobPostCRUD
from src.scraper.smartscraper import Jobs
from scrapegraphai.graphs import SmartScraperGraph

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def process_jobs(crud_instance, jobs_data):
    """Loops through job data and calls the create_job function."""
    for job in jobs_data['projects']:
        # Prepare the data for the create_job function
        job_data = {
            'id': job['id'],
            'title': job['title'],
            'description': job['description'],
            'job_type': job['job_type'],  # Ensure this matches the enum values (e.g., Fixed or Hourly)
            'experience_level': job['experience_level'],
            'duration': job['duration'],
            'rate': job['rate'],
            'proposal_count': 0,  # Default value
            'payment_verified': False,  # Default value
            'country': 'Unknown',  # Default or placeholder value
            'ratings': None,  # Placeholder for missing ratings
            'spent': None,  # Placeholder for missing spent data
            'skills': None,  # Placeholder for missing skills
            'category': 'General'  # Placeholder or default category
        }

        # Call the create_job function
        result = crud_instance.create_job(job_data)

        # Print the result for each job
        logging.info(f"Processing job: {job['title']}")
        logging.info(f"Result: {result}")

def run_service():
    logging.info("Started scraping Upwork data...")
    
    # ************************************************
    # Define the configuration for the graph
    # ************************************************
    groq_api_key = st.secrets["general"]["GROQ_API_KEY"]

    graph_config = {
        "llm": {
            "api_key": groq_api_key,
            "model": "groq/llama-3.1-8b-instant",
        },
        "verbose": True,
        "headless": False,
    }

    smart_scraper_graph = SmartScraperGraph(
        prompt="List me all the jobs",
        source="https://www.upwork.com/nx/search/jobs/?nbs=1&q=ai%20chatbot%20development&page=4&per_page=10",
        schema=Jobs,
        config=graph_config,
    )

    extract_job_id = lambda href: re.search(r'_~(\d+)', href).group(1) if re.search(r'_~(\d+)', href) else None

    jobs = smart_scraper_graph.run()
    # logging.info(f"Jobs data: {jobs}")

    # Ensure jobs is iterable and contains dictionaries
    for job in jobs:
        if isinstance(job, dict) and "id" in job:
            job_id = extract_job_id(job["id"])
            if job_id:
                job["id"] = job_id
            else:
                job["id"] = ''.join(str(random.randint(0, 9)) for _ in range(21))
        else:
            logging.error(f"Unexpected job format: {job}")
            continue

    crud = JobPostCRUD()
    process_jobs(crud, {"projects": jobs})


if __name__ == "__main__":
    run_service()