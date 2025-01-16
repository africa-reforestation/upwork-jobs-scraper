import datetime
import re, os, json, logging
import pandas as pd

async def save_jobs_json_file(merged_job_data):
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    filename = f"job_{timestamp}.json"
    filepath = os.path.join(log_dir, filename)
    with open(filepath, 'w') as f:
        json.dump(merged_job_data, f, indent=4)
    return filepath

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
