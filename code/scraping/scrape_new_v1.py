
#scraping rottentomatoes

import urllib.request, json
import re
import time
import requests
import os
import math
from db_connect import db_connect
from pymongo import errors as pme
from pymongo import MongoClient

os.chdir('code/scraping')

def read(file):
    # Same function as before (read query inputs)
    with open(file, 'r') as fh:
        return fh.read().strip().split('\n')



def calculate_pages(counts):
    """
    This function calculates the number of pages that will need to be looped through
    param counts: dict
    return: number of pages
    """
    if counts['count'] == 0:
        return 0
    else:
        return math.ceil(counts['total']/32)
    

def generate_url(genre, pagenum):
    """
    This function generates the url to be used for scraping
    param genre: genre obtained from the genre_list
    param pagenum: page number 
    return: url string
    """
    return 'https://www.rottentomatoes.com/api/private/v2.0/browse?maxTomato=100&maxPopcorn=100&services=amazon%3Bhbo_go%3Bitunes%3Bnetflix_iw%3Bvudu%3Bamazon_prime%3Bfandango_now&genres=' + str(genre) + '&sortBy=release&type=dvd-streaming-all&page=' + str(pagenum)



def scrape_counts(genre, tries):
    """
    
    """
    
    
    page_url = generate_url(genre, 1)
    for i in range(tries):
        try:
            with urllib.request.urlopen(page_url) as url:
                data=json.loads(url.read().decode())
        except Exception as e:
            time.sleep(2)
    
    
    counts = data['counts']
    results = data['results']
    
    
    
    return new_url_list






for url in l:
    
    for p in range(1,100):
        u = url + str(p)


a = scrape('genre_list.txt')











######
pageLink = 'https://www.rottentomatoes.com/api/private/v2.0/browse?maxTomato=100&maxPopcorn=100&services=amazon%3Bhbo_go%3Bitunes%3Bnetflix_iw%3Bvudu%3Bamazon_prime%3Bfandango_now&genres=2&sortBy=release&type=dvd-streaming-all&page=1'


try:
    with urllib.request.urlopen(pageLink) as url1:
        data=json.loads(url1.read().decode())
except Exception as e:
    print('failed attept')
    time.sleep(2)



counts = data['counts']
results = data['results']

a = calculate_pages(counts)
a







############
#function to calculate total
