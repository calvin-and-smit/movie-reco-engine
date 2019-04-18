import sys
sys.path.append('scraping')
sys.path.append('tools')

import read
from mp_scraper import scrape_with_mp


if __name__ == '__main__':
    # Define global variables
    initial_urls = read.by_line('../dependencies/rt_initial_urls')
    genre_codes = read.by_line('../dependencies/rt_genre_codes')

    db_credential = ['../connection-details/db-reco-engine.credential',
                     'reco-engine', 'test']

    # Start procedures
    scrape_with_mp(initial_url_list=initial_urls,
                   genre_code_list=genre_codes,
                   db_cred=db_credential,
                   worker_count1=48,
                   worker_count2=64)
