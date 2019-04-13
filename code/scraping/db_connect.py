from pymongo import MongoClient


def db_connect(db_cred, col_in_use):
    with open(db_cred, 'r', encoding='utf-8') as fhand:
        uri, db = fhand.read().strip().split('\n')
        return MongoClient(uri)[db][col_in_use]

