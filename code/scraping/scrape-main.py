from listing_scraper import lscraper
from detail_scraper import dscraper
from db_connect import db_connect
import os

# Setup working directory
os.chdir(os.path.dirname(os.path.realpath(__file__)))
os.chdir('code/scraping')

# Define final output list
final_output = list()

# Start scraping
new_urls = lscraper()
for new_url in new_urls:
    final_output.append(dscraper(new_url))

# Load data into db
col = db_connect('../../connection-details/db1.credential')
