import json
import os
import logging
from playwright.async_api import async_playwright
from src.prompts import JOB_POSTS_QUERY
from utils.data_extraction import extract_job_ids, merge_job_ids_with_data
import streamlit as st
import agentql
from utils.data_processing import save_jobs_json_file
from utils.database import push_to_postgres
from utils.login import login

os.environ["AGENTQL_API_KEY"] = st.secrets["general"]["AGENTQL_API_KEY"]

async def scrape_jobs_from_upwork(url: str):
    USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"
    MAX_RETRIES = 3
    LOG_DIR = "logs"

    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)

    async with async_playwright() as playwright:
        browser = await playwright.firefox.launch(headless=True)
        context = await browser.new_context(user_agent=USER_AGENT)
        # if not os.path.exists("upwork_login.json"):
        #     await login()
        # context = await browser.new_context(storage_state="upwork_login.json")
        page = agentql.wrap(await context.new_page())
        await page.goto(url)
        await page.wait_for_timeout(1000)
        try:
            job_posts_response = await page.query_elements(JOB_POSTS_QUERY)
            job_posts = job_posts_response.job_posts
            job_ids = await extract_job_ids(job_posts)
            job_posts_data = await job_posts.to_data()
            merged_job_data = await merge_job_ids_with_data(job_posts_data, job_ids)
            logging.info(f"merged_job_data: {merged_job_data}")
        except agentql._core._errors.AgentQLServerError:
            retries += 1
            logging.error(f"AgentQL Server Error. Retrying {retries}/{MAX_RETRIES}...")
            
    return merged_job_data

async def scrape_upwork_data(search_query, num_jobs=20):
    url = f"https://www.upwork.com/nx/search/jobs?q={search_query}&sort=recency&page=1&per_page={num_jobs}"
    merged_job_data = await scrape_jobs_from_upwork(url)
    filepath = await save_jobs_json_file(merged_job_data)
    logging.info(f"Upwork Jobs Json file saved to logs {filepath}")
    with open(filepath, "r") as file:
        jobs = json.load(file)
    await push_to_postgres(merged_job_data)
