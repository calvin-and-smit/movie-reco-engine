
#scraping rottentomatoes

import urllib.request, json
import time
import os
import math
from db_connect import db_connect
from pymongo import errors as pme
from pymongo import MongoClient


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




def calculate_pages(url, tries):
    """
    This function calculates the number of pages that will need to be looped through
    param url: the initial url where number of results can be picked up
    param tries: number of times url is requested 
    return: number of pages
    """
    for i in range(tries):
        try:
            with urllib.request.urlopen(url) as url:
                data=json.loads(url.read().decode())
        except:
            time.sleep(2)
    
    if data['count'] == 0:
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
        except:
            time.sleep(2)
    
    
    return data['counts']
    


def scrape_results(url, tries):
    """
    
    """
    
    for i in range(tries):
        try:
            with urllib.request.urlopen(url) as url:
                data=json.loads(url.read().decode())
        except:
            time.sleep(2)
    
    return data['results']


def scrape_urls(results_list, existing_url_list):
    """
    
    """
    url_list = []
    
    for r in results_list:
        if r['url'] not in existing_url_list:
            url_list.append(r['url'])
    
    return url_list


# Set relative working directory
os.chdir('code/scraping')


#genres = read('genre_list.txt')
genres = [1, 2]
#existing_url_list = get_existing_urls()

existing_url_list = []
new_list = []
for g in genres:
    u = generate_url(g, 1)
    #counts = scrape_counts(u, 5)
    p = calculate_pages(u, 5)
    for i in range(1, p+1):
        url = generate_url(g, i)
        res = scrape_results(url, 5)
        new_l = scrape_urls(res, existing_url_list)
        new_list.append(new_l)


   
# flattening the list of lists into a single usable list      
final_list = [item for sublist in new_list for item in sublist]       







