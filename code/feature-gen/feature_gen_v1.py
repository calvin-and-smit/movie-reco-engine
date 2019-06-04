
# Generating features


# Set working directory
import sys
sys.path.append('../scraping')
sys.path.append('../tools')
import db_connect
import math

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
df['Audience_Score'].fillna(0, inplace=True)
df['Audience_Score'] = pd.to_numeric(df['Audience_Score'].str.replace('%', ''), errors = 'coerce')/100


# Tomatometer
# Converting str into value between 0 and 1
df['Tomato_Meter'].fillna(0, inplace=True)
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
cols = ['Audience_Score', 'Franchise', 'Tomato_Meter', 'Scaled_Runtime', 
        'Genre_1.0', 
        'Genre_2.0', 
        'Genre_3.0', 
        'Genre_4.0', 
        'Genre_5.0', 
        'Genre_6.0', 
        'Genre_7.0', 
        'Genre_8.0', 
        'Genre_9.0', 
        'Genre_10.0', 
        'Genre_11.0', 
        'Genre_12.0', 
        'Genre_13.0', 
        'Genre_14.0', 
        'Genre_15.0', 
        'Genre_16.0', 
        'Genre_17.0', 
        'Genre_18.0', 
        'Genre_19.0', 
        'Genre_20.0', 
        'Genre_21.0']

df_knn = df_final[cols]

# function for knn
# sckit-learn algo here

# custom knn from scratch
# function to calculate distance between 2 instances
# function to return top 5 neighbors depending on the input
# https://machinelearningmastery.com/tutorial-to-implement-k-nearest-neighbors-in-python-from-scratch/
# **** this is python 2 ****

'''

def get_dist(df_record1, df_record2):
    return math.sqrt(sum((df_record1 - df_record2)**2))
    
    
def get_neighbors(test_instance, df, k):
    length_df = len(df)
    distances = list()
    for i in range(length_df):
        distances.append(get_dist(test_instance, df.iloc[i]))
    return distances
    #sort distances
    #return top k instances
    

'''




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
