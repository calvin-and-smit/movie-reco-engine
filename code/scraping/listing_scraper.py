from get_existing_urls import get_eurls
import requests
import math


def read(file):
    with open(file, 'r') as fh:
        return fh.read().strip().split('\n')


def lscraper():
    # Read dependency files
    initial_urls = read('../../dependencies/rt_initial_urls')
    genre_codes = read('../../dependencies/rt_genre_codes')
    # Load existing urls from db
    existing_urls = get_eurls()
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
                    # Load page
                    data = requests.get(url).json()
                    print('\r\n{} on page | Page {}/{} | URL: {}'.format(
                        data['counts']['count'], page_num, max_page, url))
                    # Get movie_urls on page
                    for item in data['results']:
                        if item['url'] not in existing_urls + new_urls \
                                and item['url'] != '/m/null' and len(item['url']) > 1:
                            new_urls.append(item['url'])
                            print(item['url'])
                    # Add 1 to page counter
                    page_num += 1
                else:
                    break
            continue
        continue
    # Print number of movie_urls captured
    print('\r\n{} new movie(s) found\r\n'.format(len(new_urls)))
    return new_urls


if __name__ == '__main__':
    # import os
    # os.chdir('code/scraping')
    lscraper()
