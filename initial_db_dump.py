import pandas as pd
from db_connect import db_create_engine
from sqlalchemy.orm import sessionmaker

engine = db_create_engine(config_file='config.ini',
                          conn_name='AWS_RDS')

Session = sessionmaker(bind=engine)
session = Session()


def print_col_types(table_name, conn):

    col_types = """SELECT column_name, data_type, ordinal_position 
                    FROM information_schema.columns 
                    WHERE table_name = '{}';
                    """.format(table_name)

    row_count = """SELECT count(*) as row_count
                    FROM {};
                    """.format(table_name)

    col_df = pd.read_sql_query(col_types, conn)
    row_count_df = pd.read_sql_query(row_count, conn)

    print(table_name, ': {} rows'.format(row_count_df.loc[0, 'row_count']))
    print(col_df, '\n')
    print(eval(table_name).info())


# Read in data
legislators = pd.read_pickle('data/current_legislators_df.pkl')
social = pd.read_pickle('data/legislators_social_df.pkl')
twitter_profiles = pd.read_pickle('data/twitter_profiles_df.pkl')
tweets = pd.read_pickle('data/tweets_df.pkl')


# Transform legislators datatypes and column names to match db cols
print_col_types(table_name='legislators', conn=engine)

legislators['bio.birthday'] = pd.to_datetime(legislators['bio.birthday'])
legislators.rename(columns={'id.bioguide': 'id',
                            'bio.birthday': 'birthday',
                            'bio.gender': 'gender',
                            'bio.religion': 'religion',
                            'name.first': 'first_name',
                            'name.last': 'last_name',
                            'party': 'party'}, inplace=True)

legislators.to_sql(name='legislators', con=engine, flavor='postgres', if_exists='append', index=False)


# Transform Twitter Profiles datatypes to match db cols
print_col_types(table_name='twitter_profiles', conn=engine)

twitter_profiles['id'] = [str(x) for x in twitter_profiles['id']]
twitter_profiles['created_at'] = pd.to_datetime(twitter_profiles['created_at'])
twitter_profiles['favourites_count'] = [int(x) for x in twitter_profiles['favourites_count']]
twitter_profiles['followers_count'] = [int(x) for x in twitter_profiles['followers_count']]
twitter_profiles['friends_count'] = [int(x) for x in twitter_profiles['friends_count']]
twitter_profiles['statuses_count'] = [int(x) for x in twitter_profiles['statuses_count']]

twitter_profiles.to_sql(name='twitter_profiles', con=engine, if_exists='append', index=False)


# Transform social datatypes and column names to match db cols
print_col_types(table_name='social', conn=engine)

social['social.twitter_id'] = [str(x) for x in social['social.twitter_id']]

social.rename(columns={'id.bioguide': 'legislator_id',
                       'social.facebook': 'facebook',
                       'social.twitter': 'twitter_screen_name',
                       'social.twitter_id': 'twitter_id'}, inplace=True)

social.to_sql(name='social', con=engine, if_exists='append', index=False)


# Transform Tweets datatypes to match db cols
print_col_types(table_name='tweets', conn=engine)

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

tweets.to_sql(name='tweets', con=engine, if_exists='append', index=False)
