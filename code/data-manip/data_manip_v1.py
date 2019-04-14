
#manipulating data

from db_connect import db_connect
import pandas as pd
import numpy as np


col = db_connect('../../connection-details/db2.credential')

df = pd.DataFrame(list(col.find({}))[:50])

print(df.iloc[0])

print(df.head())

print(df.describe())

print(df.columns)