from sqlalchemy import create_engine, Column, String, Integer, or_
from sqlalchemy.orm import declarative_base, sessionmaker


# Database URL
DATABASE_URL = "sqlite:///sites_crawledf.db3"
Base = declarative_base()

# Defining the SitesCrawled class
class SitesCrawled(Base):
    __tablename__ = 'sites_crawled'
    id = Column(Integer, primary_key=True)
    site = Column(String(1024))

# Defining the DoctorScrapedInfo class
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

# Defining the MultipleAddresses class
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

# Querying and deleting entries from DoctorScrapedInfo table with "PO box" or "PO BOX" in the street column
doctors_with_pobox = session.query(DoctorScrapedInfo).filter(
    or_(
        DoctorScrapedInfo.street.ilike('%PO box%'),
        DoctorScrapedInfo.street.ilike('%PO BOX%'),
        DoctorScrapedInfo.street.ilike('%PO Box%')
    )
).all()

for doctor in doctors_with_pobox:
    session.delete(doctor)

# Querying and deleting entries from MultipleAddresses table with "PO box" or "PO BOX" in the street column
addresses_with_pobox = session.query(MultipleAddresses).filter(
    or_(
        MultipleAddresses.street.ilike('%PO box%'),
        MultipleAddresses.street.ilike('%PO BOX%'),
        MultipleAddresses.street.ilike('%PO Box%')
    )
).all()

for address in addresses_with_pobox:
    session.delete(address)

# Committing the session
session.commit()
