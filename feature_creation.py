from sql_queries import tweets_sql
import pandas as pd
from db_functions import db_create_engine
from textblob import TextBlob
import re
from nltk.corpus import stopwords
stopwords.words('english')


engine = db_create_engine(config_file='config.ini',
                          conn_name='PostgresConfig')

all_tweets = pd.read_sql_query(sql=tweets_sql, con=engine)


def clean_tweet(tweet):
    """
    Utility function to clean tweet text by removing links, special characters
    using simple regex statements.
    :param tweet:
    :return:
    """
    remove_urls = re.sub(r"http\S+", "", tweet)
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])| (\w+:\ / \ / \S+)", " ", remove_urls).split())


def get_tweet_sentiment(tweet):
    """
    Utility function to classify sentiment of passed tweet
    using textblob's sentiment method
    """
    analysis = TextBlob(clean_tweet(tweet))

    if analysis.sentiment.polarity > 0:
        return 1
    elif analysis.sentiment.polarity == 0:
        return 0
    else:
        return -1


def get_tweet_normalized(tweet, stemmer):
    text = [t for t in clean_tweet(tweet).split() if t not in stopwords.words('english')]
    return [stemmer.stem(t) for t in text]


def find_rate_all_caps(tweet):
    """
    Takes in a tweet and returns the percentage of words in tweet that are all upper case
    """
    if len(tweet) == 0:
        return 0

    uppers = []
    for word in tweet.split():
        uppers.append(word.isupper())

    return sum(uppers)/len(uppers)


def generate_features(df):
    """
    Takes in a dataframe of tweets and returns new dataframe of same length with feature columns in place of original
    :param df: Dataframe with tweet data, including columns for created_at, media_type and text
    :return:
    """
    df['hour_created'] = [i.time().hour for i in df['created_at']]
    df['weekday_created'] = [i.weekday() for i in df['created_at']]
    df['photo_exists'] = [1 if 'photo' in media else 0 for media in df['media_type']]
    df['tweet_sentiment'] = [get_tweet_sentiment(tweet) for tweet in df['text']]
    df['retweets_per_followers'] = df['retweet_count']/df['user_followers']
    df['favs_per_followers'] = df['favorite_count']/df['user_followers']
    df['rate_all_caps'] = [find_rate_all_caps(i) for i in df['text']]

    df['target'] = df['party'].replace({'Republican': 1, 'Democrat': 0})

    relevant_cols = ['id', 'hour_created', 'weekday_created',
                     'photo_exists', 'tweet_sentiment', 'retweets_per_followers',
                     'favs_per_followers', 'rate_all_caps', 'retweet_count',
                     'favorite_count', 'text_length', 'target']

    drop_rows = df[df['party'] == 'Independent'].index
    df.drop(drop_rows, inplace=True)

    return df[relevant_cols], df


features, all_df = generate_features(all_tweets)


features.to_pickle('data/feature_engineering.pkl')
all_df.to_csv('data/all_feature_engineering.csv')
