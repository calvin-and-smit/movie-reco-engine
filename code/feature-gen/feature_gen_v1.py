
# Generating features


# Set working directory
import sys
sys.path.append('../scraping')
sys.path.append('../tools')
import db_connect
import math
import time
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
df['Audience_Score'].fillna('0', inplace=True)
df['Audience_Score'] = pd.to_numeric(df['Audience_Score'].str.replace('%', ''), errors = 'coerce')/100


# Tomatometer
# Converting str into value between 0 and 1
df['Tomato_Meter'].fillna('0', inplace=True)
df['Tomato_Meter'] = pd.to_numeric(df['Tomato_Meter'].str.replace('%', ''), errors = 'coerce')/100


# Movie Year
# Creating a new variable 'Movie_Yr' and applying minmax scaling
df['Movie_Yr'] = 1900
for i in range(len(df['_id'])):
    try:
        df.loc[i, 'Movie_Yr'] = pd.to_numeric(df.loc[i, 'MI_In_Theaters_1'][-4:], errors = 'coerce')
    except:
        try:
            df.loc[i, 'Movie_Yr'] = pd.to_numeric(df.loc[i, 'MI_On_Disc_1'][-4:], errors = 'coerce')
        except:
            df.loc[i, 'Movie_Yr'] = 1900
         
yr_max = max(df['Movie_Yr'])
yr_min = min(df['Movie_Yr'])
df['Scaled_Movie_Yr'] = 0.000
# for i in range(len(df['_id'])):
#     df.loc[i, 'Scaled_Movie_Yr'] = (df.loc[i, 'Movie_Yr'] - yr_min)/(yr_max - yr_min)
df['Scaled_Movie_Yr'] = (df['Movie_Yr'] - yr_min)/(yr_max - yr_min)


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
# Removing the extra content within parenthesis that explains the basis for the rating
df['MI_Rating'] = df['MI_Rating'].str.replace(r'\s+\(.*\)','')


# Cleaning up  cast names
# Firstly, replacing the NaNs with 0s
df['Casts'].fillna('NA', inplace = True)

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
# scikit-learn algo here

# custom knn from scratch
# function to calculate distance between 2 instances
# function to return top 5 neighbors depending on the input
# https://machinelearningmastery.com/tutorial-to-implement-k-nearest-neighbors-in-python-from-scratch/
# **** this is python 2 ****



def get_dist(df_record1, df_record2):
    return math.sqrt(sum((df_record1 - df_record2)**2))
    
    
def get_neighbors(test_instance, df, k):
    length_df = len(df)
    distances = list()
    for i in range(length_df):
        distances.append(get_dist(test_instance, df.iloc[i]))
    
    indices = list(np.array(distances).argsort()[1:k])    
    
    return indices
    

test_index = 11150
recos = 4
test_instance = df_knn.iloc[test_index]
ind = get_neighbors(test_instance, df_knn, recos)

print('\nInput:' + df.loc[test_index, 'Movie_Name'])
print('\nRecommendations:')
for i in ind:
    print(df.loc[i, 'Movie_Name'])



# find a way to have something like an actor index
    # this would look for actors in test and check against each actor list in train
    # what about top 10 actors? and each actor present has 0.1 weight
# Check if same director????
    
    # create separate matrix of movies x movies for actors and directors separately 
    # minmax can be done along the movie to find movies having most number of overlapping actors
    # for directors it would be a simple 1 or 0 (add weight? say 0.25)

# add a way to extract index from Movie Name
    # Need to have movie name, movie url, and index stored on the server for quick lookup



# add some context of the movie
#   - maybe extract topics from description and then compare?


#####################
    
# m1 = 'Julia Roberts, Liv Tyler, George Soros, Bono'
# m2 = 'Julia Roberts, Liv Tyler, George Soros'
# m3 = 'Tom Hanks, Adam Sand, Leo Messi, Suarez'
# m4 = 'Johnny Depp, Adam Sand, George Soros, Liv Tyler'


t0 = time.time()

#copy of the data
m = pd.DataFrame({'Movie_Name':df_final['_id'], 'Cast':df_final['Updated_Cast']})


mov_mat = pd.DataFrame(index=m['Movie_Name'], columns=['A'])
#mov_mat = mov_mat.fillna(0)

m_list = m['Movie_Name']

external_row_counter = 0
for i in m_list:
    temp_m = pd.DataFrame(index=m_list, columns=[i])
    external_cast = m.loc[external_row_counter, 'Cast'].replace(' ', '').split(',')
    internal_row_counter = 0
    for j in m_list:
        
        internal_cast = m.loc[internal_row_counter, 'Cast'].replace(' ', '').split(',')
        
        temp_sum = 0
        for ec in external_cast:
            if ec in internal_cast:
                temp_sum += 1
                
        temp_m.loc[j, i] = temp_sum/len(internal_cast)
        internal_row_counter += 1
            
    external_row_counter += 1
    mov_mat = pd.concat([mov_mat, temp_m], axis = 1, sort = False)
  


print(mov_mat.shape)
print(time.time() - t0)

# Need to figure out how to select a single column with header as the test movie
    # then that subset gets unioned with the df_knn dataframe



def get_col(df, colname):
    return df.loc[:, colname]

temp1 = get_col(mov_mat, df_final.loc[7,'_id'])

#temp1

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






