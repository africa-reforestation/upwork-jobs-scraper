import re
import logging

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
