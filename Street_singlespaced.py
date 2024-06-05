from sqlalchemy import create_engine
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
    street = Column(String)

class MultipleAddresses(Base):
    __tablename__ = 'multiple_addresses'
    id = Column(Integer, primary_key=True)
    street = Column(String)

engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

def replace_double_spaces_in_street(table_class):
    session = Session()
    try:
        # Fetch all records from the table
        all_records = session.query(table_class).all()
        for record in all_records:
            if record.street:
                # Replace double spaces with single space in street
                record.street = ' '.join(record.street.split())
        
        # Commit the changes
        session.commit()
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        session.close()

# Update street column in both tables
replace_double_spaces_in_street(DoctorScrapedInfo)
replace_double_spaces_in_street(MultipleAddresses)
