import tweepy
from math import ceil
import datetime
import time
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
    def batch_profiles(profile_ids, batch_size):
        """
        Helper method to split list of twitter accounts into batches <= batch size
        :param profile_identifier: List of twitter user_ids
        :param batch_size: Maximum number of records in each batch
        :return: A list of all twitter account batches
        """
        batches = []

        for batch_idx in range(ceil(len(profile_ids) / batch_size)):
            offset = batch_idx * batch_size
            batch = profile_ids[offset:(offset + batch_size)]
            batches.append(batch)

        return batches

    def fetch_user_profiles(self, batch_lookups):
        """
        Helper method to lookup each user in batch and return a list of the user's profile json
        :param batch_lookups: batch of twitter user_ids to lookup
        :return: list of profile data in json format
        """
        profile_list = []

        for user in self.api.lookup_users(user_ids=batch_lookups):
            profile_list.append(user._json)

        return profile_list

    def fetch_all_profiles(self, ids=[], batch_size=100):
        """
        A wrapper method around tweepy.API.lookup_users that handles the batch lookup of
          screen_names. Assuming number of screen_names < 10000, this should not typically
          run afoul of API limits

        `api` is a tweepy.API handle
        `screen_names` is a list of twitter screen names

        Returns: a list of dicts representing Twitter profiles
        """
        profiles = []
        batches = self.batch_profiles(profile_ids=ids, batch_size=batch_size)

        # Check rate limit counter
        print('Starting', self.api.rate_limit_status()['resources']['users']['/users/lookup'])

        for batch in batches:

            try:
                profiles_batch = self.fetch_user_profiles(batch_lookups=batch)
                profiles.extend(profiles_batch)

            except tweepy.error.TweepError as e:
                if e.response.status_code == 404:
                    print('404')
                    pass
                else:
                    raise e

        # Check rate limit counter
        print('Finished', self.api.rate_limit_status()['resources']['users']['/users/lookup'])

        return profiles

    @staticmethod
    def limit_handled(cursor):
        while True:
            try:
                yield cursor.next()
            except tweepy.RateLimitError:
                time.sleep(15 * 60)

    def fetch_user_timeline(self, screen_name, days_ago, include_rts=False):
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
            if (datetime.datetime.now() - tweet.created_at).days < days_ago:
                tweet_list.append(tweet._json)
            else:
                return tweet_list

    def fetch_all_timelines(self, screen_names, days_ago, include_rts=False):
        """
        Take in list of twitter screen names and fetch all tweets occurring in the past X days
        :param screen_names: list of twitter screen names
        :param days_ago: number of days to pull tweets from
        :param include_rts: boolean indicator to include retweets
        :return: list of tweets for accounts in list occurring in the past X days
        """
        timeline_list = []

        for index, name in enumerate(screen_names):

            print('({})'.format(index), name)
            try:
                timeline = self.fetch_user_timeline(screen_name=name, days_ago=days_ago,
                                                    include_rts=include_rts)

                if timeline:
                    print('{} tweets in past {} days'.format(len(timeline), days_ago))
                    timeline_list.extend(timeline)

            except tweepy.error.TweepError as e:
                if e.response.status_code == 404:
                    pass
                else:
                    raise e

        return timeline_list


def drop_tables(table_list, engine):

    for table_name in table_list:
        eval(table_name).__table__.drop(engine)
    return True

