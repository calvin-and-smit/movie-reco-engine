from db_connect import db_connect
from pymongo import errors as pme
import requests
import math


def read(file):
    with open(file, 'r') as fh:
        return fh.read().strip().split('\n')


def get_existing_urls():
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


def lscraper():
    # Read dependency files
    initial_urls = read('../../dependencies/rt_initial_urls')
    genre_codes = read('../../dependencies/rt_genre_codes')
    # Load existing urls from db
    existing_urls = get_existing_urls()
    # Define other variables
    new_urls = list()
    # Scrape on
    for initial_url in initial_urls:
        for genre_code in genre_codes:
            page_num = 1
            # Knit url
            i_url = initial_url.format(genre=genre_code, page=1)
            # Learn page limit
            max_page = math.ceil(requests.get(i_url).json()['counts']['total'] / 32)
            # Go for launch!
            while True:
                if page_num <= max_page:
                    # Knit url, again
                    url = initial_url.format(genre=genre_code, page=page_num)
                    data = requests.get(url).json()
                    print('\r\n{} on page | Page {}/{} | URL: {}'.format(
                        data['counts']['count'], page_num, max_page, url))
                    # Get movie_urls
                    for item in data['results']:
                        if item['url'] not in existing_urls + new_urls:
                            if item['url'] != '/m/null':
                                new_urls.append(item['url'])
                                print(item['url'])
                    page_num += 1
                else:
                    break
            continue
        continue
    # Print number of movie_urls captured
    print('\r\n{} new movie(s) found\r\n'.format(len(new_urls)))
    return new_urls


if __name__ == '__main__':
    # Setup working directory
    # import os
    # os.chdir('code/scraping')
    lscraper()
