import os
import re
from sqlalchemy import and_, func


from sqlalchemy import create_engine, Column, Integer, String, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
# 1. Identify duplicate rows based on "street" and "doctor_scraped_info_id", while trimming the street name
from sqlalchemy import desc, func, distinct

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



# Step 1: Identify duplicates based on name and street from the DoctorScrapedInfo table
duplicates = session.query(
    DoctorScrapedInfo.name,
    DoctorScrapedInfo.street,
    func.count(DoctorScrapedInfo.id).label('count')
).group_by(
    DoctorScrapedInfo.name, 
    DoctorScrapedInfo.street
).having(
    func.count(DoctorScrapedInfo.id) > 1
).all()

# Step 2: For each group of duplicates, identify records to be removed (except the first one)

to_remove_ids = []

for duplicate in duplicates:
    duplicate_records = session.query(DoctorScrapedInfo).filter(
        DoctorScrapedInfo.name == duplicate.name, 
        DoctorScrapedInfo.street == duplicate.street
    ).order_by(DoctorScrapedInfo.id).all()
    
    # Skip the first record and mark others for removal
    for record in duplicate_records[1:]:
        to_remove_ids.append(record.id)

# Step 3: Fetch the associated addresses from the MultipleAddresses table for these duplicates
associated_addresses = session.query(MultipleAddresses).filter(
    MultipleAddresses.doctor_scraped_info_id.in_(to_remove_ids)
).all()

# If you want to remove these duplicate DoctorScrapedInfo records and their associated addresses:
for address in associated_addresses:
    session.delete(address)

for doctor_id in to_remove_ids:
    doctor = session.query(DoctorScrapedInfo).get(doctor_id)
    session.delete(doctor)

session.commit()


##In the provided script, we first identified duplicate records in the DoctorScrapedInfo table based on the name and street columns. After pinpointing these duplicates, we determined which specific entries to remove, ensuring to keep only one unique entry for each set of duplicates. Following that, for each of these marked duplicate doctor entries, we fetched their associated address records from the MultipleAddresses table. Finally, we deleted the identified duplicate doctor records and their corresponding addresses from the database, ensuring a cleaner and de-duplicated dataset.