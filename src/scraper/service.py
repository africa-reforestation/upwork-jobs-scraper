import time
import logging
import os
from dotenv import load_dotenv
from scrapegraphai.graphs import SmartScraperGraph
from scraper.jobpostcrud import JobPostCRUD
from scraper.smartscraper import Jobs

load_dotenv()

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

    groq_api_key = os.getenv("GROQ_API_KEY")

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

    jobs = smart_scraper_graph.run()

    crud = JobPostCRUD() 
    process_jobs(crud, jobs)

if __name__ == "__main__":
    run_service()