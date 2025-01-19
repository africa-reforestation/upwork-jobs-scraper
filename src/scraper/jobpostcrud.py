from typing import Optional
import streamlit as st
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float, Boolean, Text, Enum as SQLAlchemyEnum
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import create_engine
from enum import Enum
from pydantic import BaseModel, ValidationError

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
    title: str
    description: str
    job_type: JobType
    experience_level: str
    duration: str
    rate: Optional[str]
    proposal_count: Optional[int] = 0
    payment_verified: Optional[bool] = False
    country: str
    ratings: Optional[float]
    spent: Optional[float]
    skills: Optional[str]
    category: str

class JobPostCRUD:
    def __init__(self):
        # Streamlit connection setup
        self.conn = st.connection("neon", type="sql")
        self.engine = create_engine(self.conn._connection_string)  # Use the connection string from Streamlit
        self.Session = sessionmaker(bind=self.engine)
        Base.metadata.create_all(self.engine)

    def create_job(self, job_data: dict):
        """Create a new job entry."""
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
        
