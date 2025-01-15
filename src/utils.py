import datetime
import json
import os
import re
import time
import agentql
import asyncio
import tqdm
import logging
import litellm
import html2text
import pandas as pd
from playwright.sync_api import sync_playwright
from src.prompts import SCRAPER_PROMPT_TEMPLATE
from litellm import completion
from dotenv import load_dotenv
from src.models import JobInformation, UpworkJobs
import streamlit as st


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


async def scroll_to_bottom(page, delay=1000):
    previous_height = await page.evaluate("document.body.scrollHeight")
    while True:
        await page.mouse.wheel(0, 1000)
        await page.wait_for_timeout(delay)
        new_height = await page.evaluate("document.body.scrollHeight")
        if new_height == previous_height:
            break
        previous_height = new_height

async def call_chatcompletion_api(prompt, model):
    load_dotenv()
    os.environ['GEMINI_API_KEY'] = os.getenv("GEMINI_KEY")
    litellm.enable_json_schema_validation = True
    model_name = "gemini/gemini-1.5-flash"
    try:
        response = completion(
            model=model_name, 
            messages=[
            {"role": "user", "content": prompt}
        ],
        response_format=model,
        )
        logging.info(f"Response: {response}")
        # Check response structure
        if hasattr(response, "usage"):
            token_counts = {
                "input_tokens": response.usage.prompt_tokens,
                "output_tokens": response.usage.completion_tokens,
            }
        else:
            token_counts = None
        # Parse output
        try:
            output = json.loads(response.choices[0].message.content)
        except json.JSONDecodeError:
            output = response.choices[0].message.content
        
        return output, token_counts
    except Exception as e:
        logging.error(f"Error during API call: {e}")
        return None, None
    
async def extract_job_ids(aql_response):
    job_ids = []
    for job in aql_response:
        try:
            href = await job.title.get_attribute("href")
            if href is None:
                logging.warning(f"Could not extract job ID, href is None")
                job_ids.append(None)
            elif isinstance(href, str):
                match = re.search(r'_~(\d+)', href)
                if match:
                    job_ids.append({"job_id": match.group(1)})
                else:
                    logging.warning(f"Could not extract job ID from href: {href}")
                    job_ids.append(None)
            else:
                logging.warning(f"Could not extract job ID, href is not a string: {href}")
                job_ids.append(None)
        except Exception as e:
            logging.warning(f"Could not extract job ID: {e}")
            job_ids.append(None)
    return job_ids

async def merge_job_ids_with_data(job_posts_data, job_ids):
    if len(job_posts_data) != len(job_ids):
        logging.error("Mismatch in lengths of job_posts_data and job_ids")
        return []
    merged_data = []
    for job_data, job_id in zip(job_posts_data, job_ids):
        if job_id is not None:
            job_data_with_id = {**job_data, **job_id}
        else:
            job_data_with_id = job_data
        merged_data.append(job_data_with_id)
    return merged_data

async def push_to_postgres(job_posts_data):
    for job in job_posts_data:
        try:
            # Skip data with a null or missing job_id
            if not job.get('job_id'):
                logging.warning(f"Skipping job due to missing job_id: {json.dumps(job, indent=2)}")
                continue

            # Extract and clean job_type
            job_type_raw = job.get('job_type', '').strip()
            rate = None  # Default rate value
            if "Hourly" in job_type_raw:
                job_type = "Hourly"
                duration = job.get('duration')
                # Extract rate if available in the job_type field
                rate_match = re.search(r'\$([\d.]+)-\$([\d.]+)', job_type_raw)
                if rate_match:
                    rate = f"${rate_match.group(1)}-${rate_match.group(2)}"  # Use extracted range
                else:
                    rate_match = re.search(r'\$([\d.]+)', job_type_raw)
                    if rate_match:
                        rate = f"${rate_match.group(1)}"  # Use single rate if range isn't provided
                    else:
                        rate = "$0"  # Default rate for "Hourly" without pricing
            elif "Fixed-price" in job_type_raw:
                job_type = "Fixed-price"
                # Check the rate field; fallback to duration if rate is null
                rate = job.get('rate')
                duration = job.get('duration')
                if not rate:
                    rate = duration
                    duration = None
                else:
                    if rate == "Fixed-price":
                        rate = "$0"
                        duration = None
                    else:
                        duration = None

            else:
                logging.warning(f"Invalid or missing job_type: {job_type_raw}. Skipping job.")
                continue

            # Prepare the job data
            job_data = {
                'id': job.get('job_id'),
                'title': job.get('title'),
                'description': job.get('description'),
                'job_type': job_type,  # Use the cleaned job_type
                'experience_level': job.get('experience_level'),
                'duration': duration,  # Handle missing duration
                'rate': rate,  # Use extracted or fallback rate
                'proposal_count': str(job.get('proposal_count', '')),  # Convert to string
                'payment_verified': 'Payment verified' if job.get('payment_verified') else 'Not verified',  # String
                'country': job.get('country') if job.get('country') else None,  # Handle missing country
                'ratings': str(re.search(r'\d+(\.\d+)?', job.get('ratings', '0')).group()),  # Convert to string
                'spent': str(job.get('spent', '0').replace('$', '').replace('+', '').strip()),  # String
                'skills': job.get('skills') if job.get('skills') else None,  # Pass as None if empty
                'category': job.get('category'),  # Null if not provided
            }

            # Debug: Log the data and types being sent
            logging.info(f"Data being sent: {json.dumps(job_data, indent=2)}")
            # logging.info(f"Data types: {', '.join(f'{k}: {type(v)}' for k, v in job_data.items())}")

            # Insert into the database
            # Initialize connection.
            conn = st.connection("neon", type="sql")

            # Perform query.
            query = f"""
            INSERT INTO JobPost (
                id,
                title,
                description,
                job_type,
                experience_level,
                duration,
                rate,
                proposal_count,
                payment_verified,
                country,
                ratings,
                spent,
                skills,
                category
            ) VALUES (
                '{job_data['id']}',
                '{job_data['title'].replace("'", "''")}', -- Escape single quotes
                '{job_data['description'].replace("'", "''")}',
                '{job_data['job_type']}',
                '{job_data['experience_level']}',
                {f"'{job_data['duration']}'" if job_data['duration'] else 'NULL'},
                {f"'{job_data['rate']}'" if job_data['rate'] else 'NULL'},
                '{job_data['proposal_count']}',
                '{job_data['payment_verified']}',
                {f"'{job_data['country']}'" if job_data['country'] else 'NULL'},
                '{job_data['ratings']}',
                '{job_data['spent']}',
                {f"'{job_data['skills']}'" if job_data['skills'] else 'NULL'},
                {f"'{job_data['category']}'" if job_data['category'] else 'NULL'}
            );
            """
            df = conn.query(query, ttl="10m")



        except Exception as e:
            # Log the error with detailed context
            logging.error(f"Error pushing to Postgres: {e}. Data: {json.dumps(job, indent=2)}")
            raise

async def scrape_jobs_from_upwork(url: str) -> str:
    USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"
    MAX_RETRIES = 3
    RETRY_DELAY = 2000
    JOB_POSTS_QUERY = """
    {
        job_posts[]{
            title
            description
            job_type
            experience_level
            duration
            rate
            proposal_count
            payment_verified
            country
            ratings
            spent
            skills
        }
    }
    """
    async with  sync_playwright() as playwright:
        browser = playwright.firefox.launch(headless=True)
        context = browser.new_context(user_agent=USER_AGENT)

        page = context.new_page()
        page.goto(url)
        html_content = page.content()

        browser.close()

        status = True
        while status:
            current_page = page.url
            await page.wait_for_timeout(1000)
            await scroll_to_bottom(page)
            await page.wait_for_timeout(1000)
            retries = 0
            while retries < MAX_RETRIES:
                try:
                    job_posts_response = await page.query_elements(JOB_POSTS_QUERY)
                    job_posts = job_posts_response.job_posts
                    job_ids = await extract_job_ids(job_posts)
                    job_posts_data = await job_posts.to_data()
                    merged_job_data = await merge_job_ids_with_data(job_posts_data, job_ids)
                    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
                    log_dir = "logs"
                    if not os.path.exists(log_dir):
                        os.makedirs(log_dir)
                    filename = f"job_{timestamp}.json"
                    filepath = os.path.join(log_dir, filename)
                    with open(filepath, 'w') as f:
                        json.dump(merged_job_data, f, indent=4)
                    logging.info(f"Data saved to {filepath}")
                    await page.wait_for_timeout(1000)
                    await push_to_postgres(merged_job_data)
                    logging.info("Data pushed to Postgres completed.")
                    break
                except agentql._core._errors.AgentQLServerError:
                    retries += 1
                    logging.error(f"AgentQL Server Error. Retrying {retries}/{MAX_RETRIES}...")
                    if retries < MAX_RETRIES:
                        await asyncio.sleep(RETRY_DELAY / 1000)
                    else:
                        logging.error("Max retries reached. Exiting.")

    return "jobs"

async def scrape_upwork_data(search_query, num_jobs=20):
    url = f"https://www.upwork.com/nx/search/jobs?q={search_query}&sort=recency&page=1&per_page={num_jobs}"

    markdown_content = scrape_jobs_from_upwork(url)
    prompt = SCRAPER_PROMPT_TEMPLATE.format(markdown_content=markdown_content)
    completion, _ = call_chatcompletion_api(prompt, UpworkJobs)
    jobs_links_list = [job["link"] for job in completion["jobs"]]
    logging.info(f"Jobs_links_list: {jobs_links_list}")

    # jobs_df = process_job_info_data(jobs_data)

    # return jobs_df
    return "jobs_df"


async def process_job_info_data(jobs_data):
    def clean_client_info(text):
        if pd.isna(text):
            return text

        cleaned = (
            text.replace("\n\n", " | ")
            .replace("\n", " ")
            .replace("***", "")
            .replace("**", "")
            .replace("*", "")
            .strip()
        )

        # Remove multiple spaces
        cleaned = re.sub(r"\s+", " ", cleaned)
        # Remove multiple separators
        cleaned = re.sub(r"\|\s*\|", "|", cleaned)
        # Clean up spaces around separators
        cleaned = re.sub(r"\s*\|\s*", " | ", cleaned)

        return cleaned.strip()

    jobs_df = pd.DataFrame(jobs_data)
    jobs_df["rate"] = jobs_df["rate"].str.replace(
        r"\$?(\d+\.?\d*)\s*\n*-\n*\$?(\d+\.?\d*)", r"$\1-$\2", regex=True
    )
    jobs_df["client_infomation"] = jobs_df["client_infomation"].apply(clean_client_info)

    return jobs_df
