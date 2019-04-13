import sys
sys.path.append('scraping')
sys.path.append('tools')
from movie_url_scraper_v2 import get_urls
from detail_scraper import dscrape
from db_connect import db_connect
import read
import time


if __name__ == '__main__':
    # Record start time
    start_time = time.time()
    # Define variables
    db_cred_fpath, col_in_use = '../connection-details/db-reco-engine.credential', 'production'
    # Start scraping
    new_urls = get_urls(db_connect(db_cred=db_cred_fpath, col_in_use=col_in_use),
                        genre_list=read.by_line('../dependencies/genre_list.txt'),
                        browse_list=read.by_line('../dependencies/browse_type.txt'))
    for new_url in new_urls:
        # Print progress
        print('\r\n(#{}/{}) '.format(new_urls.index(new_url) + 1, len(new_urls)), end='')
        # Scrape detail page
        data = dscrape(new_url)
        # Upload to db
        if data:
            db_connect(db_cred=db_cred_fpath, col_in_use=col_in_use).insert_one(data)
        continue
    # Print total run time
    print('\r\nRun time: {} seconds\r\n'.format(int(time.time() - start_time)))
