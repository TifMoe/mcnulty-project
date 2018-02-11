from flask import Flask
import os
import click
import pandas as pd
import yaml
import tweepy
from configparser import ConfigParser
from math import ceil
import datetime
import time

app = Flask(__name__)


@app.cli.command()
def initial_data_gather():

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
                print('batch size:', len(batch))

                try:
                    profiles_batch = self.fetch_user_profiles(batch_lookups=batch)
                    profiles.extend(profiles_batch)
                    print('profile len:', len(profiles))

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
            Take in
            :param screen_names:
            :param days_ago:
            :param include_rts:
            :return:
            """
            timeline_list = []
            print('Starting', self.api.rate_limit_status()['resources']['statuses']['/statuses/user_timeline'])

            for index, name in enumerate(screen_names):

                print(index, name)
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

            print('Finish', self.api.rate_limit_status()['resources']['statuses']['/statuses/user_timeline'])

            return timeline_list

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

    with open('congress-legislators/legislators-current.yaml', 'r') as f:
        current_legis = pd.io.json.json_normalize(yaml.load(f))

    with open('congress-legislators/legislators-social-media.yaml', 'r') as f:
        social = pd.io.json.json_normalize(yaml.load(f))

    # Pickle legislator data in dataframe format
    current_legis.to_pickle('data/current_legislators_df.pkl')
    social.to_pickle('data/legislators_social_df.pkl')

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


if __name__ == '__main__':
    initial_data_gather()




