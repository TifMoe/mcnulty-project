from ..data.db_functions import TwAPI, create_dataframes_from_tweet_json
from ..features.feature_functions import generate_features
from configparser import ConfigParser
import pandas as pd
import numpy as np


def extract_twitter_id(url):
    """
    Function to confirm valid twitter URL and extract tweet ID
    :param url: URL to tweet
    :return: twitter id for tweet
    """
    if 'twitter.com/' in url:
        status_id = url.partition("/status/")[2]
        try:
            int(status_id)
            return str(status_id)
        except ValueError:
            print('Please submit URL for particular tweet (ending in /status/somenumber)')

    else:
        print('Please submit valid Twitter URL for particular tweet (longform url, not bitly)')


def fetch_tweet_info(url):
    """
    Fetch tweet info for feature generation and display data from given url
    :param url: Url input from form
    :return: dataframe with tweet data for feature generation and list of display data
    """
    tweet_id = extract_twitter_id(url)

    config = ConfigParser()
    config.read('config.ini')

    api = TwAPI(consumer_key=config.get('TwitterKeys', 'consumer_key'),
                consumer_secret=config.get('TwitterKeys', 'consumer_secret'),
                access_token=config.get('TwitterKeys', 'access_token'),
                access_token_secret=config.get('TwitterKeys', 'access_token_secret'))

    tweet = api.api.get_status('{}'.format(tweet_id), tweet_mode="extended")
    return tweet._json


def generate_tweet_features(tweet_json):
    """
    Function to generate base features for prediction on non-text features
    :param tweet_json: json with tweet info
    :return: feature array for prediction and dictionary with tweet display info
    """

    _, tweet_df = create_dataframes_from_tweet_json(tweet_json)

    name = tweet_json['user']['name']
    profile_image = tweet_json['user']['profile_image_url_https']
    profile_banner = tweet_json['user']['profile_banner_url']
    tweet_text = tweet_json['full_text']

    display_info = {'name': name, 'profile_image': profile_image,
                    'profile_banner': profile_banner, 'tweet_text': tweet_text}

    tweet_df['user_followers'] = tweet_df['user']['followers_count']
    tweet_df['created_at'] = pd.to_datetime(tweet_df['created_at'])
    tweet_df.rename(columns={'id': 'tweet_id',
                             'user.screen_name': 'twitter_screen_name',
                             'full_text': 'text'}, inplace=True)

    features, _ = generate_features(df=tweet_df)

    return np.array(features), display_info
