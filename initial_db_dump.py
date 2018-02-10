import pandas as pd
from db_connect import db_create_engine

engine = db_create_engine(config_file='config.ini',
                          conn_name='AWS_RDS')

# Read in data and transform datatypes to make corresponding postgres columns
legislators = pd.read_pickle('data/current_legislators_df.pkl')
social = pd.read_pickle('data/legislators_social_df.pkl')
twitter_profiles = pd.read_pickle('data/twitter_profiles_df.pkl')
tweets = pd.read_pickle('data/tweets_df.pkl')