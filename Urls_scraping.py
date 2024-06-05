import threading
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import requests
from bs4 import BeautifulSoup
import time

def extract_urls_from_page(soup, target_zipcodes):
    ul_element = soup.find("ul", class_="resultslist-content")
    urls = []

    if ul_element:
        for list_item in ul_element.find_all("li"):
            a_tag = list_item.find("a")
            if a_tag:
                url = a_tag.get("href")

                response = requests.get(url)
                if response.status_code == 200:
                    inner_soup = BeautifulSoup(response.content, 'html.parser')
                    zips_on_page = inner_soup.findAll('span', {'class': "location-zipcode loc-coi-loczip"})
                    for zip_span in zips_on_page:
                        if zip_span.text.strip() in target_zipcodes:
                            urls.append(url)
                            break
    return urls

def extract_and_save_urls(base_url, db_url):
    # Database setup for each thread
    Base = declarative_base()

    class SitesCrawled(Base):
        __tablename__ = 'sites_crawled'
        id = Column(Integer, primary_key=True)
        site = Column(String(1024))

    engine = create_engine(db_url)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    driver = webdriver.Chrome()
    driver.maximize_window()
    driver.get(base_url)

    target_zipcodes = [
    '00501', '06390', '11701', '11702', '11703', '11704', '11705', '11706', 
    '11707', '11708', '11713', '11715', '11716', '11717', '11718', '11719', 
    '11720', '11721', '11722', '11724', '11725', '11726', '11727', '11729', 
    '11730', '11731', '11733', '11734', '11738', '11739', '11740', '11741', 
    '11742', '11743', '11745', '11746', '11747', '11749', '11750', '11751', 
    '11752', '11754', '11755', '11757', '11760', '11763', '11764', '11766', 
    '11767', '11768', '11769', '11770', '11772', '11775', '11776', '11777', 
    '11778', '11779', '11780', '11782', '11784', '11786', '11787', '11788', 
    '11789', '11790', '11792', '11794', '11795', '11796', '11798', '11901', 
    '11930', '11931', '11932', '11933', '11934', '11935', '11937', '11939', 
    '11940', '11941', '11942', '11944', '11946', '11947', '11948', '11949', 
    '11950', '11951', '11952', '11953', '11954', '11955', '11956', '11957', 
    '11958', '11959', '11960', '11961', '11962', '11963', '11964', '11965', 
    '11967', '11968', '11969', '11970', '11971', '11972', '11973', '11975', 
    '11976', '11977', '11978', '11980'
]

    try:
        while True:
            time.sleep(3)
            # Close ads (if any)

            soup = BeautifulSoup(driver.page_source, 'html.parser')
            current_page_urls = extract_urls_from_page(soup, target_zipcodes)

            for url in current_page_urls:
                existing_entry = session.query(SitesCrawled).filter_by(site=url).first()
                if not existing_entry:
                    entry = SitesCrawled(site=url)
                    session.add(entry)
                    session.commit()

            try:
                next_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, ".btn-next[title='Next Page']"))
                )
                driver.execute_script("arguments[0].click();", next_button)
            except (NoSuchElementException, TimeoutException):
                break
    finally:
        driver.quit()
        session.close()

# Base URLs and Database URLs
base_urls = [
    "https://doctor.webmd.com/providers/new-york/islip",
    "https://doctor.webmd.com/providers/new-york/southold",
    "https://doctor.webmd.com/providers/new-york/babylon",
    "https://doctor.webmd.com/providers/new-york/brookhaven",
    "https://doctor.webmd.com/providers/new-york/huntington",
    "https://doctor.webmd.com/providers/new-york/smithtown",
    "https://doctor.webmd.com/providers/new-york/shelter-island",
    "https://doctor.webmd.com/providers/new-york/east-hampton",
    "https://doctor.webmd.com/providers/new-york/southampton",
    "https://doctor.webmd.com/providers/new-york/riverhead"
]

db_urls = [
    "sqlite:///sites_crawled1.db3",
    "sqlite:///sites_crawled2.db3",
    "sqlite:///sites_crawled3.db3",
    "sqlite:///sites_crawled4.db3",
    "sqlite:///sites_crawled5.db3",
    "sqlite:///sites_crawled6.db3",
    "sqlite:///sites_crawled7.db3",
    "sqlite:///sites_crawled8.db3",
    "sqlite:///sites_crawled9.db3",
    "sqlite:///sites_crawled10.db3",
    
]

# Create and start threads
threads = []
for i in range(len(base_urls)):
    thread = threading.Thread(target=extract_and_save_urls, args=(base_urls[i], db_urls[i]))
    threads.append(thread)
    thread.start()

# Wait for all threads to finish
for thread in threads:
    thread.join()

print("All URLs processed and saved to the respective databases.")
