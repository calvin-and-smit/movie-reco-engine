
#scraping rottentomatoes

import urllib.request, json
import re
import time
import requests
import os
import math
from pymongo import errors as pme
from pymongo import MongoClient


def read(file):
    # Same function as before (read query inputs)
    with open(file, 'r') as fh:
        return fh.read().strip().split('\n')


def db_connect():
    with open('db.credential', 'r', encoding='utf-8') as fhand:
        uri, db, col = fhand.read().strip().split('\n')
        return MongoClient(uri)[db][col]



def calculate_pages(counts):
    """
    This function calculates the number of pages that will need to be looped through
    param counts: dict
    return: number of pages
    """
    if counts['count'] == 0:
        return 0
    else:
        return int(counts['total']/32) + 1
    



pageLink = 'https://www.rottentomatoes.com/api/private/v2.0/browse?maxTomato=100&maxPopcorn=100&services=amazon%3Bhbo_go%3Bitunes%3Bnetflix_iw%3Bvudu%3Bamazon_prime%3Bfandango_now&genres=1&sortBy=release&type=dvd-streaming-all&page=1'


try:
    with urllib.request.urlopen(pageLink) as url1:
        data=json.loads(url1.read().decode())
except Exception as e:
    print('failed attept')
    time.sleep(2)



counts = data['counts']
results = data['results']

a = calculate_pages(counts)








############
#function to calculate total
