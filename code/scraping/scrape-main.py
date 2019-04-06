import os
from db_connect import *

# Setup working directory to script's location
os.chdir('/Users/CalvinCao/Resilio Sync/GitHub/reco-engine/code/scraping')
# os.chdir(os.path.dirname(os.path.realpath(__file__)))

test_db = '../../connection/db1.credential'
col = db_connect(test_db)
