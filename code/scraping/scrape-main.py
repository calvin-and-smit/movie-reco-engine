import os
from db_connect import db_connect
from detail_scraper import dscraper
from listing_scraper import lscraper

# Setup working directory
# os.chdir(os.path.dirname(os.path.realpath(__file__)))
# os.chdir('code/scraping')
new_urls = lscraper()
