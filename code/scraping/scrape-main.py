from movie_url_scraper_v2 import get_urls
from detail_scraper import dscrape
from db_connect import db_connect
import time
import read


if __name__ == '__main__':
    # Record start time
    start_time = time.time()
    # Start scraping
    new_urls = get_urls(read.by_line('../../dependencies/genre_list.txt'),
                        read.by_line('../../dependencies/browse_type.txt'))
    for new_url in new_urls:
        # Print progress
        print('\r\n(#{}/{}) '.format(new_urls.index(new_url) + 1, len(new_urls)), end='')
        # Scrape detail page
        data = dscrape(new_url)
        # Upload to db
        if data:
            db_connect(db_cred='../../connection-details/db-reco-engine.credential',
                       col_in_use='production').insert_one(data)
        continue
    # Print total run time
    print('\r\nRun time: {} seconds\r\n'.format(int(time.time() - start_time)))
