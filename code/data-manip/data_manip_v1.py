
#manipulating data

import sys
sys.path.append('scraping')
sys.path.append('tools')
import db_connect


import pandas as pd
import numpy as np

# Define Database connection detail
db_cred_fpath  = '../../connection-details/db-reco-engine.credential'
db_in_use = 'reco-engine'
col_in_use = 'production'

# Get data from Database
col = db_connect.get_collection(db_cred=db_cred_fpath, db=db_in_use, collection=col_in_use)

# Data Manipulation
df = pd.DataFrame(list(col.find({}))[:50])

print(df.iloc[0])

print(df.head())

print(df.describe())

print(df.columns)
