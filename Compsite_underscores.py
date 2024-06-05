from sqlalchemy import create_engine, update
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String

DATABASE_URL = "sqlite:///sites_crawledf.db3"

# Define your Base class
Base = declarative_base()

# Define your tables as classes
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
    composite_key = Column(String(1024))

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
    composite_key = Column(String(1024))

# Create a database engine
engine = create_engine(DATABASE_URL)

# Create a Session class
Session = sessionmaker(bind=engine)

def replace_spaces_in_composite_key(table_class):
    session = Session()
    try:
        # Fetch all records from the table
        all_records = session.query(table_class).all()
        for record in all_records:
            if record.composite_key:
                # Replace spaces with underscores in composite_key
                record.composite_key = record.composite_key.replace(' ', '_')
        
        # Commit the changes
        session.commit()
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        session.close()

# Update composite_key in both tables
replace_spaces_in_composite_key(DoctorScrapedInfo)
replace_spaces_in_composite_key(MultipleAddresses)
