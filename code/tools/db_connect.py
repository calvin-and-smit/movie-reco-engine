from pymongo import MongoClient


def get_collection(db_cred, db_in_use, col_in_use):
    with open(db_cred, 'r', encoding='utf-8') as fhand:
        uri = fhand.read().strip()
        return MongoClient(uri)[db_in_use][col_in_use]

