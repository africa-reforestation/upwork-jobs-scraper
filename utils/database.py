import json
import re
import logging
import streamlit as st
from sqlalchemy.sql import text

async def initialize_database():
    conn = st.connection("neon", type="sql")
    check_query = "SELECT to_regclass('public.JobPost');"  # Check if table exists
    result = conn.query(check_query)
    logging.info(f"Checking if table 'JobPost' exists... :{result}")
    if result.empty:
        # Table does not exist; create it
        create_table_query = """
        CREATE TABLE public.JobPost (
            id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            description TEXT NOT NULL,
            job_type TEXT NOT NULL,
            experience_level TEXT,
            duration TEXT,
            rate TEXT,
            proposal_count TEXT,
            payment_verified TEXT,
            country TEXT,
            ratings TEXT,
            spent TEXT,
            skills TEXT,
            category TEXT
        );
        """
        conn.query(create_table_query)
        logging.info(f"Table 'JobPost' created successfully.")
    else:
        logging.info(f"Table 'JobPost' already exists.")

async def push_to_postgres(job_posts_data):
    conn = st.connection("neon", type="sql")  # Establish connection outside the loop

    await initialize_database()  # Ensure the table exists before inserting data

    with conn.session as session:  # Use a session for executing queries
        for job in job_posts_data:
            try:
                # Skip data with a null or missing job_id
                job_id = job.get("job_id")
                if not job_id:
                    continue

                # Extract and clean job_type
                job_type_raw = job.get("job_type", "").strip() if job.get("job_type") else ""
                job_type = None
                rate = None
                duration = None

                if "Hourly" in job_type_raw:
                    job_type = "Hourly"
                    duration = job.get("duration", "").strip() if job.get("duration") else None

                    # Extract rate if available in the job_type field
                    rate_match = re.search(r"\$([\d.]+)-\$([\d.]+)", job_type_raw)
                    if rate_match:
                        rate = f"${rate_match.group(1)}-${rate_match.group(2)}"
                    else:
                        rate_match = re.search(r"\$([\d.]+)", job_type_raw)
                        rate = f"${rate_match.group(1)}" if rate_match else "$0"

                elif "Fixed price" in job_type_raw or "Fixed-price" in job_type_raw:
                    job_type = "Fixed-price"
                    rate = job.get("rate", "").strip() if job.get("rate") else "$0"
                    duration = None

                else:
                    logging.warning(f"Invalid or missing job_type: {job_type_raw}. Skipping job.")
                    continue

                # Prepare the job data
                job_data = {
                    "id": job_id,
                    "title": job.get("title", ""),
                    "description": job.get("description", ""),
                    "job_type": job_type,
                    "experience_level": job.get("experience_level", ""),
                    "duration": duration,
                    "rate": rate,
                    "proposal_count": str(job.get("proposal_count", "")) if job.get("proposal_count") else "",
                    "payment_verified": "Payment verified" if job.get("payment_verified") else "Not verified",
                    "country": job.get("country"),
                    "ratings": (
                        str(re.search(r"\d+(\.\d+)?", job.get("ratings", "0")).group()) if job.get("ratings") else "0"
                    ),
                    "spent": str(job.get("spent", "0").replace("$", "").replace("+", "").strip())
                    if job.get("spent")
                    else "0",
                    "skills": job.get("skills"),
                    "category": job.get("category"),
                }

                # Debug: Log the data being sent
                logging.info(f"Data being sent: {json.dumps(job_data, indent=2)}")

                # Insert data using a parameterized query
                query = text("""
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
                    :id,
                    :title,
                    :description,
                    :job_type,
                    :experience_level,
                    :duration,
                    :rate,
                    :proposal_count,
                    :payment_verified,
                    :country,
                    :ratings,
                    :spent,
                    :skills,
                    :category
                );
                """)

                session.execute(query, job_data)  # Use parameterized data for insertion

            except Exception as e:
                # Log the error with detailed context
                logging.error(f"Error pushing to Postgres: {e}. Data: {json.dumps(job, indent=2)}")
                continue  # Skip to the next job post

        # Commit the transaction after all inserts
        session.commit()

    # Clear Streamlit cache to fetch updated data
    st.cache_data.clear()
    st.rerun()