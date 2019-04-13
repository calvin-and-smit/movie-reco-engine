from pymongo import MongoClient


def db_connect(db_cred):
    with open(db_cred, 'r', encoding='utf-8') as fhand:
        uri, db, col = fhand.read().strip().split('\n')
        return MongoClient(uri)[db][col]

