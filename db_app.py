from flask import Flask
from db_functions import TwAPI, db_create_engine
import pandas as pd
import numpy as np
import yaml
from sqlalchemy.ext.declarative import declarative_base
from configparser import ConfigParser
from sqlalchemy.dialects.postgresql import INTEGER, VARCHAR, DATE, FLOAT
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy import Column, ForeignKey


app = Flask(__name__)


@app.cli.command()
def initial_data_gather():
    """
    Gathers past 30 days legislator twitter data
    """

    def profile_dataframe(profile_json):
        """
        Takes in a list of twitter profile data in json format and returns a dataframe containing
        relevant information
        :param profile_json: List of twitter profile data in json formats
        :return: Dataframe with columns for the id, created date, description, follower/tweet counts,
            location and name
        """
        profiles_df = pd.DataFrame(columns=['id', 'created_at', 'screen_name',
                                            'description', 'location',
                                            'favourites_count', 'followers_count',
                                            'friends_count', 'statuses_count'])

        for i, json in enumerate(profile_json):
            profiles_df.loc[i, "id"] = profile_json[i]['id']
            profiles_df.loc[i, "created_at"] = profile_json[i]['created_at']
            profiles_df.loc[i, "screen_name"] = profile_json[i]['screen_name']
            profiles_df.loc[i, "description"] = profile_json[i]['description']
            profiles_df.loc[i, "location"] = profile_json[i]['location']
            profiles_df.loc[i, "favourites_count"] = profile_json[i]['favourites_count']
            profiles_df.loc[i, "followers_count"] = profile_json[i]['followers_count']
            profiles_df.loc[i, "friends_count"] = profile_json[i]['friends_count']
            profiles_df.loc[i, "statuses_count"] = profile_json[i]['statuses_count']

        return profiles_df

    def tweets_dataframe(tweets_json):
        """
        Takes in a list of user tweet data in json format and returns a dataframe containing
        relevant information
        :param tweets_json: List of tweet data in json formats
        :return: Dataframe with columns for the id, created date, description, follower/tweet counts,
            location and name
        """
        tweets_df = pd.DataFrame(columns=['tweet_id', 'user_id', 'user_name', 'created_at', 'hashtags',
                                          'tweet_text', 'favorite_count', 'retweet_count', 'followers_count'])

        for i, json in enumerate(tweets_json):
            print(i)
            tweets_df.loc[i, "tweet_id"] = tweets_json[i]['id']
            tweets_df.loc[i, "user_id"] = tweets_json[i]['user']['id']
            tweets_df.loc[i, "user_name"] = tweets_json[i]['user']['screen_name']
            tweets_df.loc[i, "created_at"] = tweets_json[i]['created_at']
            tweets_df.loc[i, "hashtags"] = tweets_json[i]['entities']['hashtags']
            tweets_df.loc[i, "tweet_text"] = tweets_json[i]['full_text']
            tweets_df.loc[i, "favorite_count"] = tweets_json[i]['favorite_count']
            tweets_df.loc[i, "retweet_count"] = tweets_json[i]['retweet_count']
            tweets_df.loc[i, "followers_count"] = tweets_json[i]['user']['followers_count']

        return tweets_df

    # Import legislator YAML files as pandas dataframes
    print('Begin data gathering...')

    with open('congress-legislators/legislators-current.yaml', 'r') as f:
        current_legis = pd.io.json.json_normalize(yaml.load(f))

    with open('congress-legislators/legislators-social-media.yaml', 'r') as f:
        social = pd.io.json.json_normalize(yaml.load(f))

    # Subset relevant columns and create target column
    current_legis['party'] = [term[0]['party'] for term in current_legis['terms']]
    legis_cols = ['id.bioguide', 'bio.birthday', 'bio.gender', 'bio.religion',
                  'name.first', 'name.last', 'party']
    social_cols = ['id.bioguide', 'social.facebook', 'social.twitter', 'social.twitter_id']

    # Pickle data in dataframe format
    current_legis[legis_cols].to_pickle('data/current_legislators_df.pkl')
    social[social_cols].to_pickle('data/legislators_social_df.pkl')

    # Fetch corresponding Twitter data for legislators over past 30 days

    config = ConfigParser()
    config.read('config.ini')

    # Instantiate Twitter API connection
    api = TwAPI(consumer_key=config.get('TwitterKeys', 'consumer_key'),
                consumer_secret=config.get('TwitterKeys', 'consumer_secret'),
                access_token=config.get('TwitterKeys', 'access_token'),
                access_token_secret=config.get('TwitterKeys', 'access_token_secret'))

    # Subset social data to only include those with valid twitter id
    twitter_social = social.dropna(subset=['social.twitter_id'])
    id_ints = [int(x) for x in twitter_social['social.twitter_id']]
    tw_names = list(twitter_social['social.twitter'])

    # Fetch profile data and pickle in dataframe format
    profiles = api.fetch_all_profiles(ids=id_ints)
    profiles_df = profile_dataframe(profiles)
    profiles_df.to_pickle('data/twitter_profiles_df.pkl')

    # Fetch twitter timeline data and pickle in dataframe format
    time_lines = api.fetch_all_timelines(screen_names=tw_names, days_ago=30, include_rts=False)
    tweets_df = tweets_dataframe(time_lines)
    tweets_df.to_pickle('data/tweets_df.pkl')

    print('Pickled data completed!')


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
        twitter_accounts = relationship('TwitterProfiles', foreign_keys=['id'],
                                        primaryjoin='twitter_profiles.id == social.twitter_id')

    class TwitterProfiles(Base):
        __tablename__ = 'twitter_profiles'
        id = Column(VARCHAR(250), primary_key=True)
        created_at = Column(DATE)
        screen_name = Column(VARCHAR(250))
        description = Column(VARCHAR(250))
        location = Column(VARCHAR(250))
        favourites_count = Column(INTEGER)
        followers_count = Column(INTEGER)
        friends_count = Column(INTEGER)
        statuses_count = Column(INTEGER)

        tweets = relationship('Tweets')

    class Tweets(Base):
        __tablename__ = 'tweets'
        id = Column(FLOAT, primary_key=True)
        twitter_id = Column(VARCHAR(250))
        twitter_screen_name = Column(VARCHAR(250))
        created_at = Column(DATE)
        hashtags = Column(VARCHAR(500))
        text = Column(VARCHAR(500))
        favorite_count = Column(INTEGER)
        retweet_count = Column(INTEGER)
        followers_count = Column(INTEGER)

        # foreign key defined here to avoid foreign key constraint - not all twitter handles will have valid profiles
        twitter_accounts = relationship('TwitterProfiles', foreign_keys=['twitter_screen_name'],
                                        primaryjoin='tweets.twitter_screen_name == twitter_profiles.screen_name')

    Base.metadata.create_all(engine)

    print('Transforming data for ingest now...')

    # Read in data
    legislators = pd.read_pickle('data/current_legislators_df.pkl')
    social = pd.read_pickle('data/legislators_social_df.pkl')
    twitter_profiles = pd.read_pickle('data/twitter_profiles_df.pkl')
    tweets = pd.read_pickle('data/tweets_df.pkl')

    # Populate Legislator table
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

    # Populate Twitter Profiles table
    twitter_profiles['id'] = [str(x) for x in twitter_profiles['id']]
    twitter_profiles['created_at'] = pd.to_datetime(twitter_profiles['created_at'])
    twitter_profiles['favourites_count'] = [int(x) for x in twitter_profiles['favourites_count']]
    twitter_profiles['followers_count'] = [int(x) for x in twitter_profiles['followers_count']]
    twitter_profiles['friends_count'] = [int(x) for x in twitter_profiles['friends_count']]
    twitter_profiles['statuses_count'] = [int(x) for x in twitter_profiles['statuses_count']]

    print('Populating Twitter Profiles Table')
    twitter_profiles.to_sql(name='twitter_profiles', con=engine, if_exists='append', index=False)

    # Populate Social table
    social['social.twitter_id'] = [str(int(x)) if not np.isnan(x) else None for x in social['social.twitter_id']]

    social.rename(columns={'id.bioguide': 'legislator_id',
                           'social.facebook': 'facebook',
                           'social.twitter': 'twitter_screen_name',
                           'social.twitter_id': 'twitter_id'}, inplace=True)

    print('Populating Social Table')
    social.to_sql(name='social', con=engine, if_exists='append', index=False)

    # Populate Tweet table
    tweets['tweet_id'] = [float(x) for x in tweets['tweet_id']]
    tweets['user_id'] = [str(x) for x in tweets['user_id']]
    tweets['created_at'] = pd.to_datetime(tweets['created_at'])
    tweets['favorite_count'] = [int(x) for x in tweets['favorite_count']]
    tweets['retweet_count'] = [int(x) for x in tweets['retweet_count']]
    tweets['followers_count'] = [int(x) for x in tweets['followers_count']]

    # Parse out hashtag text into list
    def list_hashtags(list_dicts):
        hash_list = []

        for dict in list_dicts:
            hash_list.append(dict['text'])

        return hash_list

    tweets['hashtags'] = [list_hashtags(tag) for tag in tweets['hashtags']]

    tweets.rename(columns={'tweet_id': 'id',
                           'user_id': 'twitter_id',
                           'user_name': 'twitter_screen_name',
                           'tweet_text': 'text'}, inplace=True)

    print('Populating Tweets Table (this may take several minutes... like 30)')
    tweets.to_sql(name='tweets', con=engine, if_exists='append', index=False)

    session.close_all()
    print('Successfully created!')


if __name__ == '__main__':
    initial_data_gather()
    initial_data_load_db()



