import logging
import random
import re
from typing import Any, Dict, List, Optional
from pydantic import ValidationError
import streamlit as st
from src.scraper.jobpostcrud import JobInformation, JobPostCRUD
from src.scraper.smartscraper import Jobs
from scrapegraphai.graphs import SmartScraperGraph

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def preprocess_job_data(raw_jobs_data: List[dict]) -> List[dict]:
    """
    Preprocess raw job data by validating and normalizing the job fields.

    Args:
        raw_jobs_data (List[dict]): List of raw job data.

    Returns:
        List[dict]: List of processed job data.
    """
    processed_jobs = []
    extract_job_id = lambda href: re.search(r'_~(\d+)', href).group(1) if re.search(r'_~(\d+)', href) else None

    for job in raw_jobs_data.get("projects", []):
        if isinstance(job, dict):
            job_id = extract_job_id(job.get("id", ""))
            if job_id:
                job["id"] = job_id
            else:
                # Generate a random fallback ID if job_id is invalid
                job["id"] = ''.join(str(random.randint(0, 9)) for _ in range(21))
            
            processed_jobs.append(job)
        else:
            logging.error(f"Unexpected job format: {job}")
            continue

    return processed_jobs


def process_jobs(processed_jobs_data: Dict[str, Any]) -> None:
    """
    Process and validate jobs from raw data.
    """
    crud = JobPostCRUD()
    for job in processed_jobs_data.get("projects", []):
        logging.info(f"Processing job: {job}")
        try:
            # Preprocess the job data
            preprocessed_data = preprocess_job_data(job)

            # Custom validation of job data
            validated_data = validate_job_data(preprocessed_data)

            # Convert to Pydantic model for further validation and processing
            job_info = JobInformation(**validated_data)
            
            # Save the job to the database
            result = crud.create_job(job_info.dict())
            logging.info(f"Processed job: {job['title']}, Result: {result}")

        except ValidationError as e:
            logging.error(f"Validation error for job: {job['title']}, Error: {e.errors()}")
        except ValueError as e:
            logging.error(f"Custom validation error for job: {job['title']}, Error: {str(e)}")
        except Exception as e:
            logging.error(f"Unexpected error for job: {job['title']}, Error: {str(e)}")

def validate_job_data(job):
    required_keys = {'id', 'title', 'date_time', 'description', 'job_type', 
                     'experience_level', 'duration', 'rate', 'client_infomation'}
    if not isinstance(job, dict):
        raise TypeError(f"Job entry must be a dictionary, got {type(job)}")
    missing_keys = required_keys - job.keys()
    if missing_keys:
        raise ValueError(f"Missing keys in job entry: {missing_keys}")
    return True

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

    # Fetch and parse the data
    jobs_data = smart_scraper_graph.run()
    logging.info(f"Raw jobs data: {jobs_data}")

    # Ensure jobs_data is structured as expected
    if not isinstance(jobs_data, dict) or "projects" not in jobs_data:
        logging.error("Unexpected jobs data structure. Expected a dictionary with 'projects' key.")
        return

    preprocessed_data = preprocess_job_data(jobs_data)
    logging.info(f"Preprocessed jobs data: {preprocessed_data}")


if __name__ == "__main__":
    run_service()