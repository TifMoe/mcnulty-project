from flask import Flask
import pandas as pd
import numpy as np
import pickle
from datetime import datetime, timedelta
import gzip
import yaml
from sqlalchemy.ext.declarative import declarative_base
from configparser import ConfigParser
from sqlalchemy.dialects.postgresql import INTEGER, VARCHAR, DATE, FLOAT
from sqlalchemy.types import DateTime
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy import Column, ForeignKey
from src.data.db_functions import TwAPI, db_create_engine, create_dataframes_from_tweet_json


app = Flask(__name__)


@app.cli.command()
def initial_data_gather():
    """
    Gathers past 30 days legislator twitter data
    """

    # Import legislator YAML files as pandas dataframes
    print('Begin data gathering...')
    with open('congress-legislators/legislators-current.yaml', 'r') as f:
        current_legis = pd.io.json.json_normalize(yaml.load(f))
    with open('congress-legislators/legislators-social-media.yaml', 'r') as f:
        social = pd.io.json.json_normalize(yaml.load(f))

    # Create target column and subset relevant columns
    current_legis['party'] = [term[0]['party'] for term in current_legis['terms']]
    legis_cols = ['id.bioguide', 'bio.birthday', 'bio.gender', 'bio.religion',
                  'name.first', 'name.last', 'party']

    social_cols = ['id.bioguide', 'social.facebook', 'social.twitter', 'social.twitter_id']

    # Pickle data in dataframe format
    current_legis[legis_cols].to_pickle('data/interim/current_legislators_df.pkl')
    social[social_cols].to_pickle('data/interim/legislators_social_df.pkl')
    print('Legislator data pickled!')

    def pickle_legislator_tweets(config_file, list_screen_names, last_date):

        # Fetch corresponding Twitter data for legislators over past 30 days
        config = ConfigParser()
        config.read(config_file)

        # Instantiate Twitter API connection
        api = TwAPI(consumer_key=config.get('TwitterKeys', 'consumer_key'),
                    consumer_secret=config.get('TwitterKeys', 'consumer_secret'),
                    access_token=config.get('TwitterKeys', 'access_token'),
                    access_token_secret=config.get('TwitterKeys', 'access_token_secret'))

        # Fetch twitter timeline data and pickle in dataframe format
        time_lines = api.fetch_all_timelines(screen_names=list_screen_names,
                                             last_date=last_date,
                                             include_rts=False)

        with gzip.open('data/raw/raw_tweets.pickle', 'wb') as file:
            pickle.dump(time_lines, file)

        users_df, tweets_df = create_dataframes_from_tweet_json(time_lines)
        tweets_df.to_pickle('data/interim/tweets_df.pkl')
        users_df.to_pickle('data/interim/users_df.pkl')

        print('Pickled data completed!')

    # Subset social data to only include those with valid twitter id
    twitter_social = social.dropna(subset=['social.twitter_id'])
    list_names = list(twitter_social['social.twitter'])
    month_ago = datetime.now() - timedelta(days=30)

    print('Fetching Twitter data now...')
    pickle_legislator_tweets(config_file='config.ini',
                             list_screen_names=list_names,
                             last_date=month_ago)


@app.cli.command()
def initial_data_load_db():
    """
    Populates db with legislator twitter data
    """

    connection_name = input("Name config details in 'config.ini' file: ")

    Base = declarative_base()
    engine = db_create_engine(config_file='config.ini',
                              conn_name=connection_name)

    Session = sessionmaker(bind=engine)
    session = Session()

    print('Defining table classes now...')

    class Legislators(Base):
        __tablename__ = 'legislators'
        id = Column(VARCHAR(250), index=True, primary_key=True)
        birthday = Column(DATE)
        gender = Column(VARCHAR(1))
        religion = Column(VARCHAR(250))
        first_name = Column(VARCHAR(250))
        last_name = Column(VARCHAR(250))
        party = Column(VARCHAR(250))

        social_accounts = relationship('Social')

    class Social(Base):
        __tablename__ = 'social'
        legislator_id = Column(VARCHAR(250), ForeignKey('legislators.id'), primary_key=True)
        facebook = Column(VARCHAR(250))
        twitter_screen_name = Column(VARCHAR(250))
        twitter_id = Column(VARCHAR(250))

        # foreign key defined here to avoid foreign key constraint - not all twitter handles will have valid profiles
        twitter_accounts = relationship('Profile_Log', foreign_keys=['id'],
                                        primaryjoin='user_profile_log.id == social.twitter_id')

    class Profile_Log(Base):
        __tablename__ = 'user_profile_log'
        id = Column(VARCHAR(250))
        created_at = Column(DateTime)
        screen_name = Column(VARCHAR(250), index=True,  primary_key=True)
        description = Column(VARCHAR(250))
        location = Column(VARCHAR(250))
        favourites_count = Column(INTEGER)
        followers_count = Column(INTEGER)
        friends_count = Column(INTEGER)
        statuses_count = Column(INTEGER)
        profile_image_url = Column(VARCHAR(250))
        time_zone = Column(VARCHAR(200))
        time_collected = Column(DateTime, index=True,  primary_key=True)

    class Tweets(Base):
        __tablename__ = 'tweets'
        id = Column(FLOAT, primary_key=True)
        twitter_screen_name = Column(VARCHAR(250), index=True)
        created_at = Column(DateTime)
        hashtags = Column(VARCHAR(300))
        text = Column(VARCHAR(500))
        favorite_count = Column(INTEGER)
        retweet_count = Column(INTEGER)
        followers_count = Column(INTEGER)
        lang = Column(VARCHAR)
        text_length = Column(INTEGER)
        media_type = Column(VARCHAR(200))
        user_mentions = Column(VARCHAR(250))
        time_collected = Column(DateTime)

        profiles = relationship('Profile_Log')

    Base.metadata.create_all(engine)

    print('Transforming data for ingest now...')

    # Read in data
    legislators = pd.read_pickle('data/interim/current_legislators_df.pkl')
    social = pd.read_pickle('data/interim/legislators_social_df.pkl')
    user_profile_log = pd.read_pickle('data/interim/users_df.pkl')
    tweets = pd.read_pickle('data/interim/tweets_df.pkl')

    # Populate LEGISLATOR table
    legislators['bio.birthday'] = pd.to_datetime(legislators['bio.birthday'])
    legislators.rename(columns={'id.bioguide': 'id',
                                'bio.birthday': 'birthday',
                                'bio.gender': 'gender',
                                'bio.religion': 'religion',
                                'name.first': 'first_name',
                                'name.last': 'last_name',
                                'party': 'party'}, inplace=True)

    print('Populating Legislators Table')
    legislators.to_sql(name='legislators', con=engine, if_exists='append', index=False)

    # Populate USER PROFILE LOG table
    user_profile_log.rename(columns=lambda x: str(x)[5:], inplace=True)
    user_profile_log.rename(columns={'collected':'time_collected'}, inplace=True)
    user_profile_log['id'] = [str(x) for x in user_profile_log['id']]
    user_profile_log['created_at'] = pd.to_datetime(user_profile_log['created_at'])

    #########
    # Need to make twitter_screen_name lowercase
    #########

    print('Populating User Profile Log Table')
    user_profile_log.to_sql(name='user_profile_log', con=engine, if_exists='append', index=False)

    # Populate SOCIAL table
    social['social.twitter_id'] = [str(int(x)) if not np.isnan(x) else None for x in social['social.twitter_id']]
    #########
    # Need to make social.twitter_screen_name lowercase
    #########

    social.rename(columns={'id.bioguide': 'legislator_id',
                           'social.facebook': 'facebook',
                           'social.twitter': 'twitter_screen_name',
                           'social.twitter_id': 'twitter_id'}, inplace=True)

    print('Populating Social Table')
    social.to_sql(name='social', con=engine, if_exists='append', index=False)

    # Populate TWEET table
    tweets['id'] = [float(x) for x in tweets['id']]
    tweets['created_at'] = pd.to_datetime(tweets['created_at'])

    tweets.rename(columns={'user.screen_name': 'twitter_screen_name',
                           'full_text': 'text'}, inplace=True)

    #########
    # Need to make twitter_screen_name lowercase
    #########

    print('Populating Tweets Table (this may take several minutes... like 30)')
    tweets.to_sql(name='tweets', con=engine, if_exists='append', index=False)

    session.close_all()
    print('Database successfully created!')


if __name__ == '__main__':
    initial_data_gather()
    initial_data_load_db()
