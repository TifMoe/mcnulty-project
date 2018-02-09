import pandas as pd
import yaml
import tweepy
from configparser import ConfigParser
from math import ceil
import pprint
import datetime
import time


config = ConfigParser()
config.read('config.ini')


# Import legislator YAML files as pandas dataframes
with open('congress-legislators/legislators-current.yaml', 'r') as f:
    current_legis = pd.io.json.json_normalize(yaml.load(f))

with open('congress-legislators/legislators-social-media.yaml', 'r') as f:
    social = pd.io.json.json_normalize(yaml.load(f))

# Pickle data in dataframe format
current_legis.to_pickle('data/current_legislators_df.pkl')
social.to_pickle('data/legislators_social_df.pkl')


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
        print('Starting', api.api.rate_limit_status()['resources']['users']['/users/lookup'])

        for batch in batches:

            try:
                profiles_batch = self.fetch_user_profiles(batch_lookups=batch)
                profiles.extend(profiles_batch)

            except tweepy.error.TweepError as e:
                print('Error: Please make sure the twitter account ids are int types')
                if e.response.status_code == 404:
                    pass
                else:
                    raise e

        # Check rate limit counter
        print('Finished', api.api.rate_limit_status()['resources']['users']['/users/lookup'])

        return profiles

    def fetch_user_timeline(self, screen_name, days_ago, include_rts=False):
        """"
        Takes in a twitter screen name and returns all tweet data in json format for tweets created in the past X days
        Parameter 'include_rts' to exclude or include retweets
        Returns a list of json tweets
        """
        tweet_list=[]
        page = 1
        deadend = False

        while True:
            tweets = self.api.user_timeline(screen_name=screen_name, count = 200,
                                            page=page, tweet_mode="extended", include_rts=include_rts)

            for tweet in tweets:
                if (datetime.datetime.now() - tweet.created_at).days < days_ago:
                    # Do processing here:
                    tweet_list.append(tweet._json)
                else:
                    deadend = True
                    return tweet_list

            if not deadend:
                page += 1
                time.sleep(500)

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

        for name in screen_names:
            print(name)
            timelines = self.fetch_user_timeline(screen_name=name, days_ago=days_ago,
                                                 include_rts=include_rts)
            print(len(timelines))
            timeline_list.extend(timelines)

        print('Finish', self.api.rate_limit_status()['resources']['statuses']['/statuses/user_timeline'])

        return timeline_list


api = TwAPI(consumer_key=config.get('TwitterKeys', 'consumer_key'),
            consumer_secret=config.get('TwitterKeys', 'consumer_secret'),
            access_token=config.get('TwitterKeys', 'access_token'),
            access_token_secret=config.get('TwitterKeys', 'access_token_secret'))


def profile_dataframe(profile_json):
    """
    Takes in a list of twitter profile data in json format and returns a dataframe containing
    relevant information
    :param profile_json: List of twitter profile data in json formats
    :return: Dataframe with columns for the id, created date, description, follower/tweet counts,
        location and name
    """

    profiles_df = pd.DataFrame(columns=['id', 'created_at', 'name',
                                        'description', 'location',
                                        'favourites_count', 'followers_count',
                                        'friends_count', 'statuses_count'])

    for i, json in enumerate(profile_json):
        profiles_df.loc[i, "id"] = profile_json[i]['id']
        profiles_df.loc[i, "created_at"] = profile_json[i]['created_at']
        profiles_df.loc[i, "name"] = profile_json[i]['name']
        profiles_df.loc[i, "description"] = profile_json[i]['description']
        profiles_df.loc[i, "location"] = profile_json[i]['location']
        profiles_df.loc[i, "favourites_count"] = profile_json[i]['favourites_count']
        profiles_df.loc[i, "followers_count"] = profile_json[i]['followers_count']
        profiles_df.loc[i, "friends_count"] = profile_json[i]['friends_count']
        profiles_df.loc[i, "statuses_count"] = profile_json[i]['statuses_count']

    return profiles_df


# Subset social data with valid twitter id
twitter_social = social.dropna(subset=['social.twitter_id'])
id_ints = [int(x) for x in twitter_social['social.twitter_id']]
tw_names = list(twitter_social['social.twitter'])

profiles = api.fetch_all_profiles(ids=id_ints)
profiles_df = profile_dataframe(profiles)

# Pickle profile data
profiles_df.to_pickle('data/twitter_profiles_df.pkl')

# Get timeline data for users
timelines = api.fetch_all_timelines(screen_names=tw_names, days_ago=30, include_rts=False)




