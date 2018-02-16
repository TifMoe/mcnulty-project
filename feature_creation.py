from sql_queries import tweets_sql
import pandas as pd
from db_functions import db_create_engine
from textblob import TextBlob
import re
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
import nltk
from collections import Counter

wordnet_lemma = WordNetLemmatizer()


def remove_urls_punct(tweet):
    """
    Utility function to clean tweet text by removing links, special characters
    using simple regex statements.
    :param tweet:
    :return:
    """
    # 1. Remove all urls
    remove_urls = re.sub(r"http\S+", "", tweet)

    # 2. Remove all punctuation
    clean = re.sub(r'[^\w\s]', '', remove_urls)

    return clean


def get_tweet_sentiment(tweet):
    """
    Utility function to classify sentiment of passed tweet
    using textblob's sentiment method
    """
    analysis = TextBlob(remove_urls_punct(tweet))

    if analysis.sentiment.polarity > 0:
        return 1
    elif analysis.sentiment.polarity == 0:
        return 0
    else:
        return -1


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


def fetch_all_tweets(config_file, conn_name):
    """
    Utility function to fetch tweet data from Postgres
    :param config_file: commonly 'config.ini' - file where config details are stored
    :param conn_name: section in config file with db connection and config details
    :return: pandas dataframe with all tweets
    """
    engine = db_create_engine(config_file=config_file,
                              conn_name=conn_name)

    all_tweets = pd.read_sql_query(sql=tweets_sql, con=engine)

    return all_tweets


def pickle_base_features(tweets):
    """
    Utility function to transform and pickle raw tweet data as feature/target matrix
    :param tweets:
    :return:
    """
    features, all_df = generate_features(tweets)

    features.to_pickle('data/feature_engineering.pkl')
    all_df.to_csv('data/all_feature_engineering.csv')


def all_elements(column):
    """
    Takes a column containing a row-wise list of elements, removes any braces and splits each element
    out into a list of total elements
    """
    elements_list = []

    for row in column:
        clean = row.replace('{', '').replace('}', '')

        for element in clean.split(','):
            if element != '':
                elements_list.append(element.lower())

    return elements_list


def find_most_common_elements(list_elements, find_top_x):
    """
    Takes in list of elements and returns the top x most common
    """
    count_elements = Counter(list_elements)
    return count_elements.most_common(find_top_x)


def clean_tweets(tweets):
    cleaned_tweets = []

    for i in tweets:

        # 1. Remove all urls and punctuation
        clean = remove_urls_punct(i)

        # 2. Convert to string
        uni = [str(i) for i in clean.lower().split()]

        # 3. Remove english stop words
        stops = set(stopwords.words("english"))
        meaningful_words = [w for w in uni if not w in stops]

        # 4. Create lemmas for meaningful words
        lemmas = [wordnet_lemma.lemmatize(i) for i in meaningful_words]

        cleaned_tw = " ".join( lemmas )
        cleaned_tweets.append(cleaned_tw)

    return cleaned_tweets


def tokenize_tweets(tweets):
    tweet_tokens = []

    for i in clean_tweets(tweets):

        tokens = nltk.word_tokenize(i, "english")
        tweet_tokens.append(tokens)

    return tweet_tokens


# Find Features
def find_features(job, feature_set):
    features = {}
    for w in feature_set:
        features[w] = (w in job)

    return features

