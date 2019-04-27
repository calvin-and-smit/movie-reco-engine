import sys
sys.path.append('../scraping')
sys.path.append('../tools')

import time
import db_connect
from copy import deepcopy
from pymongo import UpdateOne
from collections import Counter


def feat_gen_directors(director_list: list):
    # Load variables from global
    global raw_data, db_to_write
    # Print status & record start time
    print('\033[1;34m\r\nFeature generation for Directors started...\033[30m')
    start_time = time.time()
    # Check for empty input list
    if len(director_list) > 0:
        # Create data_to_insert template dictionary for faster IO
        dti_template = dict((f"Dir_{director.replace(' ','-')}", 0) for director in director_list)
        # Define pending job list for bulk write
        pending_jobs = list()
        # Generate Directors feature
        for row in raw_data:
            # Use Deepcopy to create a 'deep' copy from dti_template
            # Reference: https://thispointer.com/python-how-to-copy-a-dictionary-shallow-copy-vs-deep-copy/
            data_to_insert = deepcopy(dti_template)
            # Check directors one by one
            if 'MI_Director' in row:
                data_to_insert.update(dict((f"Dir_{director.replace(' ','-')}", 1) for director in row['MI_Director']
                                           if director in director_list))
            # Append job into pending job list
            # (Option upsert=True is necessary; Equivalent to 'insert or update if exists')
            pending_jobs.append(UpdateOne({'_id': row['_id']}, {'$set': data_to_insert}, upsert=True))
        # Bulk write to the database & pretty print result
        db_connect.get_collection(db_to_write).bulk_write(pending_jobs)
        # Feature generation complete & print status & runtime
        print(f'\033[1;32m\r\nFeature generation for Directors finished (runtime: {time.time() - start_time} seconds)'
              f'\r\n\033[30m')
    # If input list is empty
    else:
        print('\033[1;31mBad director list\033[30m')
    return


def feat_gen_genres():
    # Load variables from global
    global raw_data, db_to_write
    # Print status & record start time
    print('\033[1;34m\r\nFeature generation for Genres started...\033[0;30m')
    start_time = time.time()
    # Create data_to_insert template
    dti_template = dict((f'Genre_{gen_code}', 0) for gen_code in
                        set(genre for row in raw_data for genre in row['MI_Genre']))
    # Define pending job list for bulk write
    pending_jobs = list()
    # Generate Genre feature
    for row in raw_data:
        # Check Genre for each movie(row) & update data_to_insert
        data_to_insert = deepcopy(dti_template)
        data_to_insert.update(dict((f'Genre_{genre}', 1) for genre in row['MI_Genre']))
        # Append job into pending job list
        pending_jobs.append(UpdateOne({'_id': row['_id']}, {'$set': data_to_insert}, upsert=True))
    # Bulk write to the database & pretty print result
    db_connect.get_collection(db_to_write).bulk_write(pending_jobs)
    # Feature generation complete & print status & runtime
    print(f'\033[1;32m\r\nFeature generation for Genres finished (runtime: {time.time() - start_time} seconds)'
          f'\r\n\033[30m')
    return


if __name__ == '__main__':
    # Define database details
    db_to_read = ['../../connection-details/FreeAtlas1.credential',
                  'reco-engine-1', 'RawData']
    db_to_write = ['../../connection-details/FreeAtlas1.credential',
                   'reco-engine-1', 'Features']

    # Load raw data from production collection in the reco-engine database
    raw_data = list(db_connect.get_collection(db_to_read).find({}))

    # Define directors list for Director feat gen
    unique_directors = Counter()
    for line in raw_data:
        if 'MI_Director' in line:
            for dirctr in line['MI_Director']:
                unique_directors[dirctr] += 1
    top_20_directors = list(item[0] for item in unique_directors.most_common(20))

    # Generate director feature
    feat_gen_directors(director_list=top_20_directors)

    # Generate Genre feature
    feat_gen_genres()
