import requests
from bs4 import BeautifulSoup
import re
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
import json
import undetected_chromedriver.v2 as uc
from selenium.webdriver.common.by import By
from multiprocessing import Pool
import cloudscraper
from fake_useragent import UserAgent

# Define the user agent rotator
ua = UserAgent()

street_suffixes = [' Boulevard ', ' BOULEVARD ', ' Broadway ', ' BROADWAY ', ' Building ', ' BUILDING ', ' Parkway ', ' PARKWAY ', ' Highway ', ' HIGHWAY ', ' Avenue ', ' AVENUE ', ' Street ', ' STREET ', ' Drive ', ' DRIVE ', ' Circle ', ' CIRCLE ', ' Place ', ' PLACE ', ' Avenue ', ' ROAD ', ' Road ', ' Hwy ', ' Lane ', ' LANE ', ' WAY ', ' Way ', ' Dr ', ' DR ', ' Ave, ', ' Ave ', ' AVE ', ' St ', ' ST ', ' Rd ', ' RD ', ' Hw ', ' Blvd ', ' BLVD ', ' Ln ', ' 25a ', ' 25A ', ' Pkwy ', ' PKWY ', ' Grn ', ' Ste ', ' STE ', ' Ctr ', ' HWY ', ' Plz ',' PLZ ']

def extract_address_parts_webmd(address):
    parts_pattern = r'\(([^)]*)\)'
    parts = re.findall(parts_pattern, address)
    if len(parts) < 4:
        return None, None, None, None, None

    street = parts[0].strip()
    city = parts[1].strip().rstrip(',')
    state = parts[2].strip().rstrip(',') 
    zipcode = parts[3].strip()

    street_internal = None
    for suffix in street_suffixes:
        if suffix in street:
            split_street = street.split(suffix, 1)
            if len(split_street) > 1:
                street = split_street[0] + suffix
                street_internal = split_street[1].strip().rstrip('.')
    return street, street_internal, city, state, zipcode

def parse_single_address(address_str):
    address_dict = {}
    phone_number = None

    street, street_internal, city, state, zipcode = extract_address_parts_webmd(address_str)

    if ' Ste ' in street or ' STE ' in street or ' UNIT ' in street or ' Suite ' in street or ' APT ' in street or ' Bldg ' in street:
        substring = ' Ste ' if ' Ste ' in street else 'STE ' if ' STE ' in street else ' UNIT ' if ' UNIT ' in street else ' Suite ' if ' Suite ' in street else ' APT ' if ' APT ' in street else ' Bldg '
        parts = street.split(substring, 1)
        street = parts[0].strip()
        street_internal = substring + parts[1]

    address_dict['Street'] = street
    address_dict['Street_Internal'] =street_internal
    address_dict['City'] = city
    address_dict['State'] = state
    address_dict['ZipCode'] = zipcode
    return address_dict, phone_number

def extract_name_parts(full_name):
    title, first_name, middle_name, last_name, prefix, suffix = '', '', '', '', '', ''
    titles = ["Dr", "Md"]
    prefixes = ['Jr', 'Sr', 'II', 'III', 'IV', 'V']
    suffixes = ['MD', 'PhD', 'DO', 'NPC', 'NP', 'DPM', 'DC', 'FACS','DDS', 'OD', 'PsyD', 'RNP', 'DMD']
    words = full_name.split()

    if words[0].startswith(tuple(titles)):
        title = words.pop(0).rstrip(".")

    if words and words[-1] in suffixes:
        suffix = words.pop()
        while words and words[-1].rstrip(',') in suffixes:
            suffix = words.pop().rstrip(',') + ', ' + suffix
    elif ',' in words[-1]:
        last_word = words.pop().split(',')
        if last_word[-1] in suffixes:
            suffix = last_word[-1]
            words.append(last_word[0])

    if words:
        first_name = words.pop(0)

    if words:
        last_name = words.pop().rstrip(',')

    if last_name in prefixes:
        prefix = last_name
        last_name = words.pop().rstrip(',')

    if words:
        middle_name = " ".join(words).rstrip(".")

    return title, first_name, middle_name, last_name, prefix, suffix

def scrape_doctor_info(url):
    # Use CloudScraper to bypass Cloudflare protection
    scraper = cloudscraper.create_scraper()
    headers = {'User-Agent': ua.random}  # Randomize user agent
    response = scraper.get(url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')

    doctor_name_title = soup.find('h1', {'class':'provider-full-name'})
    if doctor_name_title:
        title, first_name, middle_name, last_name, prefix, suffix = extract_name_parts(doctor_name_title.text)
    else:
        title, first_name, middle_name, last_name, prefix, suffix = 'None', 'None', 'None', 'None', 'None', 'None'

    doctor_specialty = soup.find('div', {'class':'prov-specialties-wrap'})
    doctor_education = soup.findAll('div', {"class":"education-wrapper webmd-row"})

    street_elements = soup.findAll('div', {'class': "location-address loc-coi-locad webmd-row"})
    streets = [text.get_text().strip() for text in street_elements]

    city_elements = soup.findAll('span', {'class': "location-city loc-coi-loccty"})
    cities = [text.get_text().strip() for text in city_elements]

    state_elements = soup.findAll('span', {'class': "location-state loc-coi-locsta"})
    states = [text.get_text().strip() for text in state_elements]

    zipcode_elements = soup.findAll('span', {'class': "location-zipcode loc-coi-loczip"})
    zipcodes = [text.get_text().strip() for text in zipcode_elements]

    phone_elements = soup.findAll('div', {'class': "location-phone webmd-row"})
    phones = [text.get_text().strip() for text in phone_elements]

    phones += [None] * (len(streets) - len(phones))

    address_list = [
        f"({street})({city})({state})({zipcode}){' ' + phone if phone else ''}"
        for street, city, state, zipcode, phone in zip(streets, cities, states, zipcodes, phones)
    ]

    address_str = ' || '.join(address_list)

    # If multiple addresses exist, try Selenium with undetected-chromedriver for further scraping
    if address_str.count("||") > 3:
        driver = uc.Chrome()
        driver.maximize_window()
        driver.get(url)

        try:
            ad_button1 = driver.find_element(By.XPATH, "//*[@id=\"adserved-ppg\"]/div/div/div[1]/div[2]")
            ad_button1.click()
        except:
            pass
        try:
            ad_button2 = driver.find_element(By.XPATH, "//*[@id=\"adserved-ppg\"]/div/div/div/div/div[1]")
            ad_button2.click()
        except:
            pass
        try:
            ad_button3 = driver.find_element(By.XPATH, "//*[@id=\"bottombannerad\"]/button")
            ad_button3.click()
        except:
            pass

        try:
            show_more_button = driver.find_element(By.XPATH, "//*[@id=\"office-info\"]/div/div[2]/div[2]/button")
            show_more_button.click()
        except:
            pass

        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')

        address_str = ' || '.join(address_list)
        driver.close()

    university = [text.get_text() for text in doctor_education]
    university_str = ' || '.join(university)

    doctor_data = {
        "Website": "WebMD",
        "Name": doctor_name_title.text if doctor_name_title else "N/A",
        "Title": title,
        "FirstName": first_name,
        "MiddleName": middle_name,
        "LastName": last_name,
        "Prefix": prefix,
        "Suffix": suffix,
        "Specialty": ' | '.join(doctor_specialty.text.split('  ')) if doctor_specialty else "N/A",
        "University": university_str,
        "Address":  address_str,
        "Street_Internal": None,
        "PhoneNumber": 'none'
    }

    if not '||' in doctor_data['Address']:
        parsed_address, phone_number = parse_single_address(doctor_data['Address'])
        doctor_data['Address'] = json.dumps(parsed_address)
        doctor_data['PhoneNumber'] = phone_number

    return doctor_data

def extract_url_key(url):
    match = re.search(r'([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})', url)
    return match.group(0) if match else None

DATABASE_URLS = [
    "sqlite:///sites_crawled1.db3",
    "sqlite:///sites_crawled2.db3",
    "sqlite:///sites_crawled3.db3",
    "sqlite:///sites_crawled4.db3",
    "sqlite:///sites_crawled5.db3",
    "sqlite:///sites_crawled6.db3",
    "sqlite:///sites_crawled7.db3",
    "sqlite:///sites_crawled8.db3",
    "sqlite:///sites_crawled9.db3",
    "sqlite:///sites_crawled10.db3"
]


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


    @property
    def generate_composite_key(self):
        return f"{self.first_name}_{self.last_name}_{self.street}_{self.zip_code}"

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



# Define your process_database function
def process_database(db_url):

    # SQLAlchemy setup
    engine = sa.create_engine(db_url)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    # Create a new session for each database
    session = Session()


    urls = session.query(SitesCrawled.id, SitesCrawled.site).all()

    for url in urls:
        try:
            doctor_info = scrape_doctor_info(url[1])
            if doctor_info:
                title, first_name, middle_name, last_name, prefix, suffix = extract_name_parts(doctor_info['Name'])
                street = "multiple addresses" if '||' in doctor_info['Address'] else ''
                street_internal = None
                city = None
                state = None
                zip_code = None

                if not street:
                    address_dict = json.loads(doctor_info['Address'])
                    street = address_dict.get('Street')
                    street_internal = address_dict.get('Street_Internal')
                    city = address_dict.get('City')
                    state = address_dict.get('State')
                    zip_code = address_dict.get('ZipCode')

                if street:
                    doctor = DoctorScrapedInfo(
                        site_crawled_id=url[0], site=url[1], website=doctor_info['Website'], 
                        name=doctor_info['Name'], title=title, first_name=first_name, 
                        middle_name=middle_name, last_name=last_name, personal_suffix=prefix, 
                        clinical_suffix=suffix, specialty=doctor_info['Specialty'], 
                        university=doctor_info['University'], street=street, 
                        street_internal=street_internal, city=city, 
                        state=state, zip_code=zip_code, phone_number=doctor_info['PhoneNumber']
                    )

                    # Setting composite_key and url_key after creating the doctor instance
                    doctor.composite_key = doctor.generate_composite_key
                    doctor.url_key = extract_url_key(url[1])

                    session.add(doctor)
                    session.commit()
                    session.flush()


                if '||' in doctor_info['Address']:
                    addresses = doctor_info['Address'].split('||')
                    for address in addresses:
                        address = address.strip()
                        if address:
                            phone_number = None
                            tel_index = address.find('Tel:')
                            if tel_index != -1:
                                phone_number = address[tel_index+len('Tel:'):].strip()
                                address = address[:tel_index].strip()

                            street, street_internal, city, state, zipcode = extract_address_parts_webmd(address)

                            multiple_address = MultipleAddresses(
                                doctor_scraped_info_id=doctor.id,
                                street=street, 
                                street_internal=street_internal, 
                                city=city, 
                                state=state, 
                                zip_code=zipcode, 
                                phone_number=phone_number
                            )

                            # Generate the Composite Key using data from DoctorScrapedInfo
                            composite_key = f"{doctor.first_name}_{doctor.last_name}_{street}_{zipcode}"
                            multiple_address.composite_key = composite_key

                            session.add(multiple_address)

                    session.commit()

        except Exception as e:
            print(f"Error processing URL {url[1]}: {str(e)}\n")

    session.close()

    # Ensure the multiprocessing code only runs when the script is executed directly
if __name__ == '__main__':
    # Create a pool of worker processes
    with Pool(len(DATABASE_URLS)) as pool:
        # Map the process_database function to each database URL
        pool.map(process_database, DATABASE_URLS)
