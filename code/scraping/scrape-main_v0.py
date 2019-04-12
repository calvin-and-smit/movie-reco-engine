from listing_scraper import lscraper
from detail_scraper import dscraper
from db_connect import db_connect
import time


if __name__ == '__main__':
    # Record start time
    start_time = time.time()
    # Start scraping
    new_urls = lscraper()
    for new_url in new_urls:
        # Print progress
        print('\r\n(#{}/{}) '.format(new_urls.index(new_url)+1, len(new_urls)), end='')
        # Scrape detail page
        data = dscraper(new_url)
        # Upload to db
        if data:
            db_connect('../../connection-details/db1.credential').insert_one(data)
        continue
    # Print total run time
    print('\r\nRun time: {} seconds\r\n'.format(int(time.time() - start_time)))
