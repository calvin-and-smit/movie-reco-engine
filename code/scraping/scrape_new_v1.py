
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


def get_existing_urls():
    # Get existing url list from db
    try:
        return list(i['URL'] for i in db_connect().find({}, {"URL": 1, "_id": 0}) if len(i) > 0)
    except pme.ServerSelectionTimeoutError:  # If connection timed out
        print('DB server timed out. Global_urls set to empty')
        return list()
    except ValueError:  # If db cred file content error
        print('Db.credential file content error. Global_urls set to empty')
        return list()




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




def scrape_counts(url, tries):
    """
    
    """
    
    for i in range(tries):
        try:
            with urllib.request.urlopen(url) as url:
                data=json.loads(url.read().decode())
        except Exception as e:
            time.sleep(2)
    
    
    return data['counts']
    


def scrape_results(url, tries):
    """
    
    """
    
    for i in range(tries):
        try:
            with urllib.request.urlopen(url) as url:
                data=json.loads(url.read().decode())
        except Exception as e:
            time.sleep(2)
    
    return data['results']


def scrape_urls(results_list):
    """
    
    """
    url_list = []
    existing_url_list = get_existing_urls()
    for r in results_list:
        url_list.append(r['url'])
    
    return url_list





#genres = read('genre_list.txt')
genres = [2]
new_list = []
for g in genres:
    u = generate_url(g, 1)
    counts = scrape_counts(u, 5)
    p = calculate_pages(counts)
    for i in range(1, p+1):
        url = generate_url(g, i)
        res = scrape_results(url, 5)
        new_l = scrape_urls(res)
        new_list.append(new_l)
        
        


#currently its a list of lists 
#need to change that






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
