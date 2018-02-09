import pandas as pd
import yaml
import tweepy
from configparser import ConfigParser
from math import ceil
import pprint
from time import sleep, strftime, time


config = ConfigParser()
config.read('config.ini')


# Import data files
with open('congress-legislators/legislators-current.yaml', 'r') as f:
    current_legis = pd.io.json.json_normalize(yaml.load(f))

with open('congress-legislators/legislators-social-media.yaml', 'r') as f:
    social = pd.io.json.json_normalize(yaml.load(f))


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
        self.api = tweepy.API(self.auth)

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

profiles = api.fetch_all_profiles(ids=id_ints)
profiles_df = profile_dataframe(profiles)



for status in tweepy.Cursor(tw_api.user_timeline, screen_name='RepCurbelo').items():
    print(status._json['text'])

created = []
tweet_text = []
followers = []
friends = []

for status in tweepy.Cursor(tw_api.user_timeline, screen_name='RepCurbelo').items():
    pprint.pprint(status._json)


