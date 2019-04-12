from db_connect import db_connect
from pymongo import errors as pme


def get_eurls():
    try:
        return list(i['Movie_URL'] for i in
                    db_connect('../../connection-details/db1.credential').find(
                        {}, {"Movie_URL": 1, "_id": 0}) if len(i) > 0)
    except pme.ServerSelectionTimeoutError:  # If connection timed out
        print('DB server timed out. Global_urls set to empty')
        return list()
    except ValueError:  # If db cred file content error
        print('Db.credential file content error. Global_urls set to empty')
        return list()

