from movie_url_scraper_v2 import get_urls
from detail_scraper import dscraper
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
        print('\r\n(#{}/{}) '.format(new_urls.index(new_url) ,len(new_urls)), end='')
        # Scrape detail page
        data = dscraper(new_url)
        # Upload to db
        if data:
            db_connect('../../connection-details/db1.credential').insert_one(data)
        continue
    # Print total run time
    print('\r\nRun time: {} seconds\r\n'.format(int(time.time() - start_time)))
