import os
import agentql
from playwright.async_api import async_playwright
import logging
import streamlit as st

EMAIL = st.secrets["general"]["EMAIL"]
PASSWORD = st.secrets["general"]["PASSWORD"]
os.environ["AGENTQL_API_KEY"] = st.secrets["general"]["AGENTQL_API_KEY"]

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

async def login():
    INITIAL_URL = "https://www.upwork.com/ab/account-security/login"
    EMAIL_INPUT_QUERY = """
    {
        login_form {
            email_input
            continue_btn
        }
    }
    """
    PASSWORD_INPUT_QUERY = """
    {
        login_form {
            password_input
            login_btn
        }
    }
    """
    async with async_playwright() as playwright:
        browser = await playwright.firefox.launch(headless=True)
        page = agentql.wrap(await browser.new_page())
        await page.goto(INITIAL_URL)
        email_response = await page.query_elements(EMAIL_INPUT_QUERY)
        await email_response.login_form.email_input.fill(EMAIL)
        await page.wait_for_timeout(1000)
        await email_response.login_form.continue_btn.click()
        await page.wait_for_timeout(1000)
        password_response = await page.query_elements(PASSWORD_INPUT_QUERY)
        await password_response.login_form.password_input.fill(PASSWORD)
        await page.wait_for_timeout(1000)
        await password_response.login_form.login_btn.click()
        await page.wait_for_page_ready_state()
        await browser.contexts[0].storage_state(path="upwork_login.json")
        await page.wait_for_timeout(1000)
