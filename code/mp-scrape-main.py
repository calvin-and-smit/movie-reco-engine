import sys
sys.path.append('scraping')
sys.path.append('tools')

import mp_scraper


if __name__ == '__main__':
    # Enable Logging
    # logging.basicConfig(level=logging.DEBUG)
    # Define Database Details
    db_credential = ['../../connection-details/db-reco-engine.credential',
                     'reco-engine', 'test']
    # Execute mp_scraper.main()
    mp_scraper.main(db_cred=db_credential, worker_count=64)
    pass


