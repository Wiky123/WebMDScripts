## Update the street column by appending street_internal to it
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

from sqlalchemy import or_

# Regular expression pattern to match string contains only digits and whitespaces
number_pattern = re.compile(r'^[\d\s]+$')

# Get the rows where the street contains 'road' or 'rd' (case insensitive)
addresses = session.query(MultipleAddresses).filter(
    or_(
        func.lower(MultipleAddresses.street).contains("road"),
        func.lower(MultipleAddresses.street).contains("rd")
    )
).all()

# Iterate over the selected addresses
for address in addresses:
    # Check if the street_internal contains only digits (and possibly white spaces)
    if address.street_internal and number_pattern.fullmatch(address.street_internal):
        # Update the street column by appending street_internal to it
        address.street = f"{address.street} {address.street_internal.strip()}"
        
        # Clear the street_internal column
        address.street_internal = None

# Commit the changes
session.commit()
