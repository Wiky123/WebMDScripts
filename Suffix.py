import os
import re
from sqlalchemy import and_, func


from sqlalchemy import create_engine, Column, Integer, String, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Database information
DATABASE_URL = "sqlite:///sites_crawledf.db3"

Base = declarative_base()

# SitesCrawled Table
class SitesCrawled(Base):
    __tablename__ = 'sites_crawled'
    id = Column(Integer, primary_key=True)
    site = Column(String(1024))

# DoctorScrapedInfo Table
class DoctorScrapedInfo(Base):
    __tablename__ = 'scraped_doctors_info'
    id = Column(Integer, primary_key=True)
    site_crawled_id = Column(Integer, nullable=False)
    site = Column(String(1024), nullable=False)
    website = Column(String(1024))
    name = Column(String(1024))
    title = Column(String(1024))
    first_name = Column(String(1024))
    middle_name = Column(String(1024))
    last_name = Column(String(1024))
    personal_suffix = Column(String(1024))  
    clinical_suffix = Column(String(1024))
    specialty = Column(String(1024))
    university = Column(String(1024))
    street = Column(String(1024))     
    street_internal = Column(String(1024))
    city = Column(String(1024))       
    state = Column(String(1024))       
    zip_code = Column(String(1024))    
    phone_number = Column(String(1024))

# MultipleAddresses Table
class MultipleAddresses(Base):
    __tablename__ = 'multiple_addresses'
    id = Column(Integer, primary_key=True, autoincrement=True)  # Auto-incrementing id
    doctor_scraped_info_id = Column(Integer, nullable=False)
    street = Column(String(1024))
    street_internal = Column(String(1024))
    city = Column(String(1024))
    state = Column(String(1024))
    zip_code = Column(String(1024))
    phone_number = Column(String(1024))

# Setting up the connection and session
engine = create_engine(DATABASE_URL)
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

# List of suffixes to search for
suffixes = ["DDS", "OD", "PsyD", "RNP", "DMD"]

# Query the DoctorScrapedInfo table to find relevant rows
for doctor in session.query(DoctorScrapedInfo).all():
    # Check if the last name contains any of the suffixes
    for suffix in suffixes:
        if doctor.last_name and suffix in doctor.last_name:
            # Strip out the suffix from the last name and place it in the clinical_suffix column
            doctor.last_name = doctor.last_name.replace(suffix, "").strip()
            doctor.clinical_suffix = suffix
            
            # Move the information from the middle_name column to the last_name column
            if doctor.middle_name:
                doctor.last_name = doctor.middle_name
                doctor.middle_name = None
            break

# Commit the changes to the database
session.commit()
