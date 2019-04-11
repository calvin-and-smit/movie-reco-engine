from db_connect import db_connect
from pymongo import errors as pme
from pprint import pprint
import requests
import math
import time
import os


def read(file):
    with open(file, 'r') as fh:
        return fh.read().strip().split('\n')


def get_existing_urls():
    # Get existing url list from db
    try:
        return list(i['Movie_URL'] for i in
                    db_connect('../../connection-details/db1.credential').find(
                        {}, {"Movie_URL": 1, "_id": 0}) if len(i) > 0)
    except pme.ServerSelectionTimeoutError:  # If connection timed out
        print('DB server timed out. Global_urls set to empty')
        return list()
    except ValueError:  # If db cred file content error
        print('Db.credential file content error. Global_urls set to empty')
        return list()


# Setup working directory
# os.chdir('code/scraping')
# os.chdir(os.path.dirname(os.path.realpath(__file__)))

# Read dependency files
initial_urls = read('../../dependencies/rt_initial_urls')
genre_codes = read('../../dependencies/rt_genre_codes')

# Load existing urls from db
existing_urls = get_existing_urls()

# Define other variables
new_urls = list()

# Record start time
start_time = time.time()

# Scrape on
for initial_url in initial_urls:
    for genre_code in genre_codes:
        page_num = 1
        # Knit url
        url = initial_url.format(genre=genre_code, page=page_num)
        # Learn page limit
        max_page = math.ceil(requests.get(url).json()['counts']['total']/32)
        # Go for launch!
        while True:
            if page_num <= max_page:
                # Knit url, again
                url = initial_url.format(genre=genre_code, page=page_num)
                results = requests.get(url).json()['results']
                # Get movie_urls
                for item in results:
                    if item['url'] not in existing_urls+new_urls:
                        new_urls.append(item['url'])
                        print(item['url'])
                page_num += 1
            else:
                break
        continue
    continue

# Print number of movie_urls captured
print(len(new_urls))
# Print total run time
print('\r\nRun time: {} seconds\r\n'.format(int(time.time()-start_time)))
