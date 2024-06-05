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

# 1. Identify duplicate rows based on "street" and "doctor_scraped_info_id", while trimming the street name
duplicates_subquery = (
    session.query(func.trim(MultipleAddresses.street).label('trimmed_street'), MultipleAddresses.doctor_scraped_info_id, func.count(MultipleAddresses.id).label('count'))
    .group_by(func.trim(MultipleAddresses.street), MultipleAddresses.doctor_scraped_info_id)
    .having(func.count(MultipleAddresses.id) > 1)
    .subquery()
)

# 2. Find the IDs of the rows to be deleted (all except the first occurrence)
rows_to_delete = (
    session.query(MultipleAddresses.id)
    .join(
        duplicates_subquery,
        and_(
            func.trim(MultipleAddresses.street) == duplicates_subquery.c.trimmed_street,
            MultipleAddresses.doctor_scraped_info_id == duplicates_subquery.c.doctor_scraped_info_id
        )
    )
    .order_by(MultipleAddresses.id.desc())
    .filter(MultipleAddresses.id.notin_(
        session.query(func.min(MultipleAddresses.id))
        .group_by(func.trim(MultipleAddresses.street), MultipleAddresses.doctor_scraped_info_id)
    ))
    .all()
)

# Extract ids from the result for deletion
ids_to_delete = [row[0] for row in rows_to_delete]

# 3. Delete the identified rows from the MultipleAddresses table
session.query(MultipleAddresses).filter(MultipleAddresses.id.in_(ids_to_delete)).delete(synchronize_session='fetch')
session.commit()