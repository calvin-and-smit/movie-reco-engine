
# Manipulating data


# Set working directory
import sys
sys.path.append('../scraping')
sys.path.append('../tools')
import db_connect

import pandas as pd
import numpy as np




# Define Database connection detail
db_credential = ['../../connection-details/db-reco-engine.credential',
                 'reco-engine', 'production']
# Get data from Database
col = db_connect.get_collection(db_credential)

# Data Manipulation
df = pd.DataFrame(list(col.find({})))

#print(df.iloc[8])

#a = df.iloc[55]
#type(df['Audience_Score_ur'][1])



# Franchise
# Replacing NaNs with 0 first 
df['Franchise'].fillna(0, inplace=True)
# Replacing non-0 values with 1 since they are a part of a franchise
for i in range(len(df['_id'])):
    if df['Franchise'][i] != 0:
        df['Franchise'][i] = 1
        
#df['Franchise'].value_counts()


# Movie Rating (to be used in conjunction with Genre)
# Removing the extra content within parathesis that explains the basis for the rating
df['MI_Rating'] = df['MI_Rating'].str.replace(r'\s+\(.*\)','')


# Cleaning up  cast names
# Firstly, replacing the NaNs with empty lists
df['Casts'].fillna(0, inplace = True)

#Secondly, removing the on-screen character names from the list
for i in range(len(df['_id'])):
    if df['Casts'][i] != 0:
        



#df['MI_Rating'].value_counts()

#len(df['MI_Studio'].unique())

#####





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
#### Audience Score <S>
#### Critics Score <S>
#### Franchise Flag *** <S>
#### Box Office <S>
#### Director <X>
#### Genre **** <X>
#### Director Actor Pair Flag ****
#### Actor-Actor Pair Flag
#### Rating ** <X>
#### Studio <X>
#### TomatoMeter <X>
#### Streaming Service???

## features to be used for content based cosine similarity algorithm
#### 

## features to be used for collaborative filtering algorithm
####

################################################
