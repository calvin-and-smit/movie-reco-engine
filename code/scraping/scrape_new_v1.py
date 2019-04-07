
#scraping rottentomatoes

import os
import time
from selenium import webdriver
from selenium.common import exceptions as sce
from selenium.webdriver.chrome.options import Options
from pymongo import errors as pme
from pymongo import MongoClient


