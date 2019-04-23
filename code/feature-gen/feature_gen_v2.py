import os
os.chdir('/home/windr/GitHub/reco-engine/code/feature-gen')
import sys
sys.path.append('../scraping')
sys.path.append('../tools')

import db_connect
from copy import deepcopy
from pprint import pprint
from collections import Counter


# Define database details
db_cred_prod = ['../../connection-details/db-reco-engine.credential',
                 'reco-engine', 'production']
db_cred_feat = ['../../connection-details/db-reco-engine.credential',
                'reco-engine', 'features']


# Obtain unique directors as a Counter() object from production collection in the database
with db_connect.get_collection(db_cred_prod).find({}) as prod_data:
    # Obtain unique directors list
    unique_directors = Counter()
    for row in prod_data:
        if 'MI_Director' in row:
            for director in row['MI_Director']:
                unique_directors[director] += 1
    # Verify length
    print(len(unique_directors))


# Define important directors
# (Using the top 20 most common directors extracted from database for now)
important_directors = list(item[0] for item in unique_directors.most_common(20))
# Create data_to_insert template dictionary for faster IO
dti_template = dict((f"Dir_{director.replace(' ','-')}", 0) for director in important_directors)
dti_template.update({'Other_Directors': 0})


# Generate Directors feature
with db_connect.get_collection(db_cred_prod).find({}) as prod_data:
    for row in prod_data:
        # Use Deepcopy to create a 'deep' copy from dti_template
        # Reference: https://thispointer.com/python-how-to-copy-a-dictionary-shallow-copy-vs-deep-copy/
        data_to_insert = deepcopy(dti_template)
        # Check directors one by one
        if 'MI_Director' in row:
            for director in row['MI_Director']:
                if director in important_directors:
                    data_to_insert[f"Dir_{director.replace(' ','-')}"] = 1
                elif director not in important_directors:
                    data_to_insert['Other_Directors'] = 1
            pass
        elif 'MI_Director' not in row:
            pass
        # Insert data into features collection in the database
        # Option upsert=True is necessary; Equivalent to 'insert or update if exists'
        db_connect.get_collection(db_cred_feat).update_one({'_id': row['_id']},
                                                           {'$set': data_to_insert},
                                                           upsert=True)
        # Verifying output
        # pprint(data_to_insert)
        continue


