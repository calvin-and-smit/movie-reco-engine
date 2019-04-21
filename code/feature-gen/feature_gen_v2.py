import sys
sys.path.append('../scraping')
sys.path.append('../tools')

import db_connect

# Define database details
db_credential = ['../../connection-details/db-reco-engine.credential',
                 'reco-engine', 'production']

# Obtain raw data from database
col_production = db_connect.get_collection(db_credential)


