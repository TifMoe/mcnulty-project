import pandas as pd
import gzip
import pickle
from src.data.sql_queries import last_updated_sql
import src.data.db_functions as db_funcs
from configparser import ConfigParser


def load_new_twitter_data():
    """
    Fetch tweets and updated profile data added to twitter since date database last updated
    """

    social = pd.read_pickle('data/interim/legislators_social_df.pkl')

    # Find latest update
    engine = db_funcs.db_create_engine(config_file='config.ini', conn_name='PostgresConfig')

    twitter_social = social.dropna(subset=['social.twitter_id'])
    list_names = list(twitter_social['social.twitter'])

    last_updated = pd.read_sql_query(sql=last_updated_sql, con=engine)
    last_updated_time = last_updated.iloc[0, 0]

    # Fetch corresponding Twitter data for legislators since last day fetched
    config = ConfigParser()
    config.read('config.ini')

    api = db_funcs.TwAPI(consumer_key=config.get('TwitterKeys', 'consumer_key'),
                         consumer_secret=config.get('TwitterKeys', 'consumer_secret'),
                         access_token=config.get('TwitterKeys', 'access_token'),
                         access_token_secret=config.get('TwitterKeys', 'access_token_secret'))

    # Pickle the raw tweets before transforming to dataframe in interim pickle files
    recent_tweets = api.fetch_all_timelines(screen_names=list_names,
                                            last_date=last_updated_time)
    with gzip.open('data/raw/raw_tweets.pickle', 'wb') as file:
        pickle.dump(recent_tweets, file)

    # Pickle interim data before loading into sql database
    users_df, tweets_df = db_funcs.create_dataframes_from_tweet_json(recent_tweets)
    tweets_df.to_pickle('data/interim/tweets_df.pkl')
    users_df.to_pickle('data/interim/users_df.pkl')

    # Append new data to sql database tables
    db_funcs.load_user_profile_table(df=users_df, engine=engine, if_exists='append')
    db_funcs.load_tweets_table(df=tweets_df, engine=engine, if_exists='append')

    print('Success!!!')
