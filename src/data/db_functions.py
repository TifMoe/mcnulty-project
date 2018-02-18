import tweepy
import pandas as pd
from datetime import datetime
import time
import numpy as np
from sqlalchemy import create_engine
from configparser import ConfigParser


# Connect to Postgres DB
def db_create_engine(config_file, conn_name):
    """
    Create a sqlAlchemy engine to connect to Postgres database given some connection parameters in config file.
    Note - this can be used to connect to any Postgres db either remotely or locally

    :param config_file: A config file with connection configuration details under conn_name heading
    :param conn_name: The section name for set of configuration details for desired connection
    :return: A sqlAlchemy engine connected to aws postgres database
    """
    config = ConfigParser()
    config.read(config_file)

    engine = create_engine('postgresql://{}:{}@{}:{}/{}'
                           .format(config.get('{}'.format(conn_name), 'user'),
                                   config.get('{}'.format(conn_name), 'password'),
                                   config.get('{}'.format(conn_name), 'host'),
                                   config.get('{}'.format(conn_name), 'port'),
                                   config.get('{}'.format(conn_name), 'db')))

    return engine


# Create class to access Twitter API
class TwAPI:

    def __init__(self,
                 access_token,
                 access_token_secret,
                 consumer_key,
                 consumer_secret):
        """
        Initialize api client
        """
        self.auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        self.auth.set_access_token(access_token, access_token_secret)
        self.api = tweepy.API(self.auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

    @staticmethod
    def limit_handled(cursor):
        while True:
            try:
                yield cursor.next()
            except tweepy.RateLimitError:
                time.sleep(15 * 60)

    def fetch_user_timeline(self, screen_name, last_date, include_rts=False):
        """"
        Takes in a twitter screen name and returns all tweet data in json format for tweets created in the past X days
        Parameter 'include_rts' to exclude or include retweets
        Returns a list of json tweets
        """
        tweet_list = []

        cursor = tweepy.Cursor(self.api.user_timeline, screen_name=screen_name,
                               include_rts=include_rts, tweet_mode="extended", count=200)
        tweets = self.limit_handled(cursor.items())

        for tweet in tweets:
            if tweet.created_at > last_date:
                tweet_list.append(tweet._json)
            else:
                return tweet_list

    def fetch_all_timelines(self, screen_names, last_date, include_rts=False):
        """
        Take in list of twitter screen names and fetch all tweets occurring in the past X days
        :param screen_names: list of twitter screen names
        :param days_ago: number of days to pull tweets from
        :param include_rts: boolean indicator to include retweets
        :return: list of tweets for accounts in list occurring in the past X days
        """
        timeline_list = []

        for index, name in enumerate(screen_names):

            try:
                timeline = self.fetch_user_timeline(screen_name=name, last_date=last_date,
                                                    include_rts=include_rts)
                if timeline:
                    timeline_list.extend(timeline)

            except tweepy.error.TweepError as e:
                if e.response.status_code == 404:
                    pass
                else:
                    raise e

        return timeline_list


# Parse out hashtag text into list
def list_hashtags(list_dicts):
    hash_list = []

    for dict in list_dicts:
        hash_list.append(dict['text'])

    return hash_list


# Parse out user mentions text into list
def list_mentions(list_dicts):
    mentions_list = []

    for dict in list_dicts:
        mentions_list.append(dict['screen_name'])

    return mentions_list


# Parse out media tags into list of types
def list_media(list_dicts):
    media_types = []

    # check for nan values
    if list_dicts != list_dicts:
        return []

    for dict in list_dicts:
        media_types.append(dict['type'])

    return media_types


def create_dataframes_from_tweet_json(tweet_json):
    """
    Function to transform tweet data in json format into tabular format and subset relevant
    information for tweets dataframe and users dataframe
    :param tweet_json: Json tweet data - a list of dictionaries containing tweet data
    :return: A dataframe for all relevant tweet data and a dataframe with updated user profiles
    """

    wide_tweets_df = pd.io.json.json_normalize(tweet_json)

    tweet_cols = ['created_at', 'display_text_range', 'entities.hashtags',
                  'entities.media', 'entities.user_mentions',
                  'favorite_count', 'full_text', 'id', 'lang', 'retweet_count', 'user.screen_name']

    user_cols = ['user.created_at', 'user.description', 'user.favourites_count',
                 'user.followers_count', 'user.friends_count', 'user.id',
                 'user.location',  'user.profile_image_url',
                 'user.statuses_count', 'user.time_zone']

    tweets_df = wide_tweets_df.loc[:, tweet_cols + user_cols]

    # Parse text range, hashtags, media and user mentions
    tweets_df['text_length'] = [i[1] for i in tweets_df['display_text_range']]
    tweets_df['hashtags'] = [list_hashtags(tag) for tag in tweets_df['entities.hashtags']]
    tweets_df['media_type'] = [list_media(tag) for tag in tweets_df['entities.media']]
    tweets_df['user_mentions'] = [list_mentions(tag) for tag in tweets_df['entities.user_mentions']]
    tweets_df['time_collected'] = datetime.now()

    # Cleanup
    tweets_df.drop(['display_text_range', 'entities.hashtags',
                    'entities.media', 'entities.user_mentions'], axis=1, inplace=True)
    tweets_df.reset_index(drop=True, inplace=True)

    # Break user profile data out into users dataframe and drop duplciates
    users_df = tweets_df.loc[:, user_cols + ['time_collected', 'user.screen_name']]
    users_df.drop_duplicates(keep='first',
                             subset=['time_collected', 'user.screen_name'],
                             inplace=True)
    tweets_df.drop(user_cols, axis=1, inplace=True)

    return users_df, tweets_df


def load_legislator_table(df, engine, if_exists='append'):
    """Utility function to transform dataframe to conform to database scheme and load in sql db"""

    df['bio.birthday'] = pd.to_datetime(df['bio.birthday'])
    df.rename(columns={'id.bioguide': 'legislator_id',
                                'bio.birthday': 'birthday',
                                'bio.gender': 'gender',
                                'bio.religion': 'religion',
                                'name.first': 'first_name',
                                'name.last': 'last_name',
                                'party': 'party'}, inplace=True)

    print('Populating Legislators Table')
    df.to_sql(name='legislators', con=engine, if_exists=if_exists, index=False)


def load_user_profile_table(df, engine, if_exists='append'):
    """Utility function to transform dataframe to conform to database scheme and load in sql db"""

    df.rename(columns=lambda x: str(x)[5:], inplace=True)
    df.rename(columns={'collected': 'time_collected'}, inplace=True)
    df['id'] = [str(x) for x in df['id']]
    df['created_at'] = pd.to_datetime(df['created_at'])
    df['screen_name'] = [x.lower() for x in df['screen_name']]

    df.rename(columns={'id': 'twitter_user_id'}, inplace=True)

    print('Populating User Profile Log Table')
    df.to_sql(name='user_profile_log', con=engine, if_exists=if_exists, index=False)


def load_social_table(df, engine, if_exists='append'):
    """Utility function to transform dataframe to conform to database scheme and load in sql db"""

    df['social.twitter_id'] = [str(int(x)) if not np.isnan(x) else None for x in df['social.twitter_id']]
    df['social.twitter'].fillna('', inplace=True)
    df['social.twitter'] = [x.lower() for x in df['social.twitter']]

    df.rename(columns={'id.bioguide': 'legislator_id',
                       'social.facebook': 'facebook',
                       'social.twitter': 'twitter_screen_name',
                       'social.twitter_id': 'twitter_id'}, inplace=True)

    print('Populating Social Table')
    df.to_sql(name='social', con=engine, if_exists=if_exists, index=False)


def load_tweets_table(df, engine, if_exists='append'):
    """Utility function to transform dataframe to conform to database scheme and load in sql db"""

    df['id'] = [str(x) for x in df['id']]
    df['created_at'] = pd.to_datetime(df['created_at'])
    df['user.screen_name'] = [x.lower() for x in df['user.screen_name']]

    df.rename(columns={'id': 'tweet_id',
                       'user.screen_name': 'twitter_screen_name',
                       'full_text': 'text'}, inplace=True)

    print('Populating Tweets Table (this may take several minutes... like 30)')
    df.to_sql(name='tweets', con=engine, if_exists=if_exists, index=False)


def drop_tables(table_list, engine):
    for table_name in table_list:
        eval(table_name).__table__.drop(engine)
    return True


def drop_all_tables():
    tables = ['Social', 'Legislators', 'Profile_Log', 'Tweets']
    drop_tables(tables, engine=db_create_engine(config_file='config.ini',
                                                conn_name='PostgresConfig'))

