from get_existing_urls import get_existing_urls
import requests
import time
import math


def calculate_pages(url, tries):
    """
    This function calculates the number of pages that will need to be looped through
    param url: the initial url where number of results can be picked up
    param tries: number of times url is requested
    return: number of pages
    """
    for i in range(tries):
        try:
            data = requests.get(url).json()
        except:
            time.sleep(2)
        else:
            if data['counts']['count'] == 0:
                return 0
            else:
                return math.ceil(data['counts']['total'] / 32)


def generate_url(genre, browse_type, pagenum):
    """
    This function generates the url to be used for scraping
    param genre: genre obtained from the genre_list
    param browse_type: browse type obtained from the browse_list
    param pagenum: page number
    return: url string
    """
    return 'https://www.rottentomatoes.com/api/private/v2.0/browse?maxTomato=100&maxPopcorn=100&services=amazon%3Bhbo_go%3Bitunes%3Bnetflix_iw%3Bvudu%3Bamazon_prime%3Bfandango_now&genres=' + str(
        genre) + '&sortBy=release&type=' + browse_type + '&page=' + str(pagenum)


def scrape_results(url, tries):
    """
    This function scrapes the results part of the data dictionary on each page
    param url: url of the data page
    param tries: number of times url is requested
    return: results (list)
    """
    for i in range(tries):
        try:
            data = requests.get(url).json()
        except:
            time.sleep(2)
        else:
            return data['results']
            


def parse_urls(results_list, existing_url_list):
    """
    This function parses the results to grab individual urls
    param results_list: list of results from which individual urls are to be parsed
    param existing_url_list: list of existing urls from the DB
    return: url list
    """
    url_list = list()
    for r in results_list:
        if r['url'] not in existing_url_list:
            url_list.append(r['url'])
    return url_list



def get_urls(db_connection, genre_list, browse_list):
    """
    
    """
    
    existing_url_list = get_existing_urls(db_connection=db_connection)
    url_list = list()
    for browse_type in browse_list:
        for genre in genre_list:
            initial_url = generate_url(genre, browse_type, 1)
            total_pages = calculate_pages(initial_url, 5)
            for pagenum in range(1, total_pages + 1):
                url = generate_url(genre, browse_type, pagenum)
                results = scrape_results(url, 5)
                scraped_urls = parse_urls(results, existing_url_list)
                url_list.append(scraped_urls)
    
    # flattening the list of lists into a single usable list
    final_list = list(item for sublist in url_list for item in sublist)
    
    # removing duplicates from the list
    final_url_list = list(set(final_list))
    
    return final_url_list


