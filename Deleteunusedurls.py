import logging
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Enable logging for SQLAlchemy
logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

# Database URL
DATABASE_URL = "sqlite:///sites_crawled1 copy.db3"
Base = declarative_base()

# # SitesCrawled Table
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


# Fetch all site_crawled_id values from DoctorScrapedInfo
used_site_ids = session.query(DoctorScrapedInfo.site_crawled_id).distinct().all()
used_site_ids = [id[0] for id in used_site_ids]

# Delete from SitesCrawled where id is not in used_site_ids
session.query(SitesCrawled).filter(SitesCrawled.id.notin_(used_site_ids)).delete(synchronize_session=False)

# Commit changes to the database
session.commit()

# Close the session
session.close()

print("Unused rows from SitesCrawled have been deleted.")