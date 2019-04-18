
# Manipulating data

import sys
sys.path.append('scraping')
sys.path.append('tools')
import db_connect

import pandas as pd
import numpy as np

# Set working directory


# Define Database connection detail
db_cred_fpath  = '../../connection-details/db-reco-engine.credential'
db_in_use = 'reco-engine'
col_in_use = 'production'

# Get data from Database
col = db_connect.get_collection(db_cred=db_cred_fpath, db=db_in_use, collection=col_in_use)

# Data Manipulation
df = pd.DataFrame(list(col.find({}))[:50])

#print(df.iloc[10])

a = df.iloc[8]
type(df['Audience_Score_ur'][1])



################################################################################################

########## Idea: Similarity algorithm ##########
# Ask for 3 movies that user has seen recently as input
# Find a movie closest from all 3
# Find an upcoming movie closest from all 3 (use subset of data by filtering on In_Theatres_Date)
################################################

########## Idea: Collaborative Filtering algorithm ##########
# Create user profiles (pre defined) (as future scope look into the ability to letting users select movies)
# Let user pick a profile
# Suggest 2-3 movies as recommendations
################################################



################################################

# features to be manipulated:
## features to be used for content based knn algorithm
#### Audience Score
#### Critics Score
#### Franchise Flag ***
#### Box Office
#### Director
#### Genre ****
#### Director Actor Pair Flag ****
#### Actor-Actor Pair Flag
#### Rating **
#### Studio
#### TomatoMeter
#### Streaming Service???

## features to be used for content based cosine similarity algorithm
#### 

## features to be used for collaborative filtering algorithm
####

################################################