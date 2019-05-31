
# Generating features


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


# Audience Score
# Converting str into value between 0 and 1
df['Audience_Score'] = pd.to_numeric(df['Audience_Score'].str.replace('%', ''), errors = 'coerce')/100


# Tomatometer
# Converting str into value between 0 and 1
df['Tomato_Meter'] = pd.to_numeric(df['Tomato_Meter'].str.replace('%', ''), errors = 'coerce')/100


# Runtime
# Creating a new variable 'Runtime' and applying minmax scaling
df['Runtime'] = pd.to_numeric(df['MI_Runtime_1'].str.replace(' minutes', ''), errors = 'coerce')
for i in range(len(df['_id'])):
    if df.loc[i, 'Runtime'] > 300:
        df.loc[i, 'Runtime'] = 300

runtime_max = max(df['Runtime'])
runtime_min = min(df['Runtime'])
df['Scaled_Runtime'] = 0.000
for i in range(len(df['_id'])):
    df.loc[i, 'Scaled_Runtime'] = (df.loc[i, 'Runtime'] - runtime_min)/(runtime_max - runtime_min)


# Franchise
# Replacing NaNs with 0 first 
df['Franchise'].fillna(0, inplace=True)
# Replacing non-0 values with 1 since they are a part of a franchise
for i in range(len(df['_id'])):
    if df.loc[i, 'Franchise'] != 0:
        df.loc[i, 'Franchise'] = 1
        
#df['Franchise'].value_counts()


# Movie Rating (to be used in conjunction with Genre)
# Removing the extra content within parathesis that explains the basis for the rating
df['MI_Rating'] = df['MI_Rating'].str.replace(r'\s+\(.*\)','')


# Cleaning up  cast names
# Firstly, replacing the NaNs with 0s
df['Casts'].fillna(0, inplace = True)

# Secondly, removing the on-screen character names from the list
# and also adding the new resulting cast list to the dataframe
df['Updated_Cast'] = 0

for i in range(len(df['_id'])):
    if df.loc[i, 'Casts'] != 0:
        cast_per_movie = list()
        for j in range(len(df['Casts'][i])):
            cast_per_movie.append(df['Casts'][i][j][0])
        df.loc[i, 'Updated_Cast'] = ', '.join(cast_per_movie)


# Creating dummy variables for Genres
# Using a temporary df to hold the dummies
df_temp = pd.get_dummies(df['MI_Genre'].apply(pd.Series).stack(), prefix='Genre').sum(level = 0)
df_final = pd.concat([df, df_temp], axis = 1)

# Keeping only the required columns
# Defining the variable cols to hold the required columns
cols = ['Audience_Score', 'Franchise', 'Tomato_Meter', 'Scaled_Runtime', '']



# =============================================================================
# 
# # variables for knn
# Audience_Score - done
# Franchise - done
# MI_Rating 'dummy??????????
# Tomato_Meter - done
# Scaled_Runtime - done
# Genre 'dummy
# Need to bring in cast as well 
# 
# 
# =============================================================================







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

########## Idea: Similar Description ##########
# Ask for 3 movies that user has seen recently as input
# Find a movie description closest from all 3
# Dice's coefficient can be used to find similarity. or even cosine similarity of tf-idf
################################################
        
################################################################################################
