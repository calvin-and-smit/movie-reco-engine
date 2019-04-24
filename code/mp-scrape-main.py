import sys
sys.path.append('scraping')
sys.path.append('tools')

from mp_scraper import scrape_with_mp


if __name__ == '__main__':
    # Define variables
    initial_urls_fpath = '../dependencies/rt_initial_urls'
    genre_codes_fpath = '../dependencies/rt_genre_codes'

    # Define database details
    db_credential = ['../connection-details/db-reco-engine.credential',
                     'reco-engine', 'test']

    # Start scraping procedures
    scrape_with_mp(iul_fpath=initial_urls_fpath,
                   gcl_fpath=genre_codes_fpath,
                   db_cred=db_credential,
                   worker_count1=64,
                   worker_count2=64)
