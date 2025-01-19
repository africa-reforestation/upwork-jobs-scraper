###Tutoral4
"""
Basic example of scraping pipeline using SmartScraper with schema
"""

import os
from typing import List
from enum import Enum
from dotenv import load_dotenv
from pydantic import BaseModel, Field
from typing import List, Optional
from scrapegraphai.graphs import SmartScraperGraph

load_dotenv()

# ************************************************
# Define the output schema for the graph
# ************************************************

class JobLinks(BaseModel):
    link: str = Field(description="The link to the job")
    
class UpworkJobs(BaseModel):
    jobs: List[JobLinks] = Field(description="The list of scraped jobs")

class JobType(str, Enum):
    FIXED = "Fixed"
    HOURLY = "Hourly"

class JobInformation(BaseModel):
    title: str = Field(description="The title of the job")
    date_time: str = Field(description="The Date and time the job was posted")
    description: str = Field(description="The full description of the job")
    job_type: JobType = Field(
        description="The type of the job (Fixed or Hourly)"
    )
    experience_level: str = Field(description="The experience level of the job")
    duration: str = Field(description="The duration of the job")
    rate: Optional[str] = Field(
        description="""
        The payment rate for the job. Can be in several formats:
        - Hourly rate range: '$15.00-$25.00' or '$15-$25'
        - Fixed rate: '$500' or '$1,000'
        - Budget range: '$500-$1,000'
        All values should include the '$' symbol.
        """
    )
    client_infomation: Optional[str] = Field(
        description="The description of the client including location, number of hires, total spent, etc."
    )

class Jobs(BaseModel):
    projects: List[JobInformation]

