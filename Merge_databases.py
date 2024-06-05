import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker, session
from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy as sa
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import sessionmaker, declarative_base

Base = declarative_base()

class SitesCrawled(Base):
    __tablename__ = 'sites_crawled'
    id = Column(Integer, primary_key=True)
    site = Column(String(1024))

class DoctorScrapedInfo(Base):
    __tablename__ = 'scraped_doctors_info'
    id = Column(Integer, primary_key=True)
    site_crawled_id = Column(Integer, nullable=False)
    url_key = Column(String(1024))
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
    composite_key = Column(String(1024))


class MultipleAddresses(Base):
    __tablename__ = 'multiple_addresses'
    id = Column(Integer, primary_key=True)
    doctor_scraped_info_id = Column(Integer, nullable=False)
    street = Column(String(1024))
    street_internal = Column(String(1024))
    city = Column(String(1024))
    state = Column(String(1024))
    zip_code = Column(String(1024))
    phone_number = Column(String(1024))
    composite_key = Column(String(1024))

# Establish connections to the databases
source_db_urls = [
    "sqlite:///sites_crawled8.db3"
    "sqlite:///sites_crawled9.db3",
    "sqlite:///sites_crawled10.db3"
]
destination_db_url = "sqlite:///sites_crawled9 copy.db3"

# Create engine and session for the destination database
destination_engine = sa.create_engine(destination_db_url)
DestinationSession = sessionmaker(bind=destination_engine)
dest_session = DestinationSession()

# Function to get the next available ID for a table
def get_next_id(session, model):
    max_id = session.query(sa.func.max(model.id)).scalar()
    return (max_id or 0) + 1

# Function to migrate data from a source database
def migrate_data(source_db_url, dest_session):
    source_engine = sa.create_engine(source_db_url)
    SourceSession = sessionmaker(bind=source_engine)
    source_session = SourceSession()

    # Migrate SitesCrawled
    sites_crawled_mapping = {}
    next_site_id = get_next_id(dest_session, SitesCrawled)
    for site in source_session.query(SitesCrawled).all():
        new_site = SitesCrawled(id=next_site_id, site=site.site)
        dest_session.add(new_site)
        sites_crawled_mapping[site.id] = next_site_id
        next_site_id += 1

    # Migrate DoctorScrapedInfo
    doctor_mapping = {}
    next_doctor_id = get_next_id(dest_session, DoctorScrapedInfo)
    for doctor in source_session.query(DoctorScrapedInfo).all():
        new_doctor = DoctorScrapedInfo(
            id=next_doctor_id,
            site_crawled_id=sites_crawled_mapping[doctor.site_crawled_id],  # Corrected
            url_key=doctor.url_key,
            site=doctor.site,
            website=doctor.website,
            name=doctor.name,
            title=doctor.title,
            first_name=doctor.first_name,
            middle_name=doctor.middle_name,
            last_name=doctor.last_name,
            personal_suffix=doctor.personal_suffix,
            clinical_suffix=doctor.clinical_suffix,
            specialty=doctor.specialty,
            university=doctor.university,
            street=doctor.street,
            street_internal=doctor.street_internal,
            city=doctor.city,
            state=doctor.state,
            zip_code=doctor.zip_code,
            phone_number=doctor.phone_number,
            composite_key=doctor.composite_key
            # Ensure no duplicate keyword arguments here
        )
        dest_session.add(new_doctor)
        doctor_mapping[doctor.id] = next_doctor_id
        next_doctor_id += 1

    # Migrate MultipleAddresses
    next_address_id = get_next_id(dest_session, MultipleAddresses)
    for address in source_session.query(MultipleAddresses).all():
        new_address = MultipleAddresses(
            id=next_address_id,
            doctor_scraped_info_id=doctor_mapping.get(address.doctor_scraped_info_id),  # Corrected
            street=address.street,
            street_internal=address.street_internal,
            city=address.city,
            state=address.state,
            zip_code=address.zip_code,
            phone_number=address.phone_number,
            composite_key=address.composite_key
            # Ensure no duplicate keyword arguments here
        )
        dest_session.add(new_address)
        next_address_id += 1

    source_session.close()
    dest_session.commit()

# Run the migration for each source database
for db_url in source_db_urls:
    migrate_data(db_url, dest_session)

dest_session.close()