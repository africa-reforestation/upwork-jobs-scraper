from typing import Optional
import streamlit as st
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float, Boolean, Text, Enum as SQLAlchemyEnum
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import create_engine
from enum import Enum
from pydantic import BaseModel, ValidationError, validator

Base = declarative_base()

# Enum for job types
class JobType(str, Enum):
    FIXED = "Fixed"
    HOURLY = "Hourly"

# SQLAlchemy JobPost model
class JobPost(Base):
    __tablename__ = 'jobpost'

    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String, nullable=False)
    description = Column(Text, nullable=False)
    job_type = Column(SQLAlchemyEnum(JobType), nullable=False)
    experience_level = Column(String, nullable=False)
    duration = Column(String, nullable=False)
    rate = Column(String, nullable=True)
    proposal_count = Column(Integer, default=0)
    payment_verified = Column(Boolean, default=False)
    country = Column(String, nullable=False)
    ratings = Column(Float, nullable=True)
    spent = Column(Float, nullable=True)
    skills = Column(Text, nullable=True)
    category = Column(String, nullable=False)

# Pydantic model for validation
class JobInformation(BaseModel):
    id: str
    title: str
    date_time: str
    description: str
    job_type: str
    experience_level: str
    duration: str
    rate: str
    client_information: str

    @validator("id", pre=True)
    def validate_id(cls, value):
        # Ensure ID is a string
        return str(value)

    @validator("job_type", pre=True)
    def normalize_job_type(cls, value):
        # Normalize job_type values
        if isinstance(value, str):
            value = value.lower().strip()
            if "hourly" in value:
                return "Hourly"
            if "fixed" in value:
                return "Fixed"
        raise ValueError("Invalid job type format")

class JobPostCRUD:
    def __init__(self):
        # Retrieve the connection URL from Streamlit secrets
        connection_url = st.secrets["connections"]["neon"]["url"]

        # Create the SQLAlchemy engine
        self.engine = create_engine(connection_url)

        # Configure sessionmaker
        self.Session = sessionmaker(bind=self.engine)

        # Create tables if they do not already exist
        Base.metadata.create_all(self.engine)


    def create_job(self, job_data: dict):
        """Create a new job entry."""
        session = None  # Initialize session to None
        try:
            validated_data = JobInformation(**job_data)
            new_job = JobPost(**validated_data.dict())
            session = self.Session()
            session.add(new_job)
            session.commit()
            return {"status": "success", "job_id": new_job.id}
        except ValidationError as e:
            return {"status": "error", "message": e.errors()}
        except Exception as e:
            return {"status": "error", "message": str(e)}
        finally:
            if session:  # Only close session if it was initialized
                session.close()


    def read_job(self, job_id: int):
        """Retrieve a job by ID."""
        try:
            query = f"SELECT * FROM jobpost WHERE id = {job_id};"
            df = self.conn.query(query)
            if df.empty:
                return {"status": "error", "message": "Job not found"}
            return {"status": "success", "job": df.to_dict(orient="records")[0]}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    def update_job(self, job_id: int, update_data: dict):
        """Update an existing job."""
        try:
            set_clause = ", ".join(f"{key} = '{value}'" for key, value in update_data.items())
            query = f"UPDATE jobpost SET {set_clause} WHERE id = {job_id};"
            self.conn.query(query)
            return {"status": "success", "message": "Job updated successfully"}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    def delete_job(self, job_id: int):
        """Delete a job by ID."""
        try:
            query = f"DELETE FROM jobpost WHERE id = {job_id};"
            self.conn.query(query)
            return {"status": "success", "message": "Job deleted successfully"}
        except Exception as e:
            return {"status": "error", "message": str(e)}
        
