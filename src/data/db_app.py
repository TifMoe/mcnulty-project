from flask import Flask
import pandas as pd
import pickle
from datetime import datetime, timedelta
import gzip
import yaml
from sqlalchemy.ext.declarative import declarative_base
from configparser import ConfigParser
from sqlalchemy.dialects.postgresql import INTEGER, VARCHAR, DATE
from sqlalchemy.types import DateTime
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy import Column, ForeignKey, PrimaryKeyConstraint, ForeignKeyConstraint
import src.data.db_functions as db_funcs


app = Flask(__name__)


@app.cli.command()
def initial_data_gather():
    """
    Gathers past 30 days legislator twitter data
    """

    # Import legislator YAML files as pandas dataframes
    print('Begin data gathering...')
    with open('congress-legislators/legislators-current.yaml', 'r') as f:
        current_legis = pd.io.json.json_normalize(yaml.load(f))
    with open('congress-legislators/legislators-social-media.yaml', 'r') as f:
        social = pd.io.json.json_normalize(yaml.load(f))

    # Create target column and subset relevant columns
    current_legis['party'] = [term[0]['party'] for term in current_legis['terms']]
    legis_cols = ['id.bioguide', 'bio.birthday', 'bio.gender', 'bio.religion',
                  'name.first', 'name.last', 'party']

    social_cols = ['id.bioguide', 'social.facebook', 'social.twitter', 'social.twitter_id']

    # Pickle data in dataframe format
    current_legis[legis_cols].to_pickle('data/interim/current_legislators_df.pkl')
    social[social_cols].to_pickle('data/interim/legislators_social_df.pkl')
    print('Legislator data pickled!')

    def pickle_legislator_tweets(config_file, list_screen_names, last_date):

        # Fetch corresponding Twitter data for legislators over past 30 days
        config = ConfigParser()
        config.read(config_file)

        # Instantiate Twitter API connection
        api = db_funcs.TwAPI(consumer_key=config.get('TwitterKeys', 'consumer_key'),
                             consumer_secret=config.get('TwitterKeys', 'consumer_secret'),
                             access_token=config.get('TwitterKeys', 'access_token'),
                             access_token_secret=config.get('TwitterKeys', 'access_token_secret'))

        # Fetch twitter timeline data and pickle in dataframe format
        time_lines = api.fetch_all_timelines(screen_names=list_screen_names,
                                             last_date=last_date,
                                             include_rts=False)

        with gzip.open('data/raw/raw_tweets.pickle', 'wb') as file:
            pickle.dump(time_lines, file)

        users_df, tweets_df = db_funcs.create_dataframes_from_tweet_json(time_lines)
        tweets_df.to_pickle('data/interim/tweets_df.pkl')
        users_df.to_pickle('data/interim/users_df.pkl')

        print('Pickled data completed!')

    # Subset social data to only include those with valid twitter id
    twitter_social = social.dropna(subset=['social.twitter_id'])
    list_names = list(twitter_social['social.twitter'])
    month_ago = datetime.now() - timedelta(days=30)

    print('Fetching Twitter data now...')
    pickle_legislator_tweets(config_file='config.ini',
                             list_screen_names=list_names,
                             last_date=month_ago)


@app.cli.command()
def initial_data_load_db():
    """
    Populates db with legislator twitter data
    """

    connection_name = input("Name config details in 'config.ini' file: ")

    Base = declarative_base()
    engine = db_funcs.db_create_engine(config_file='config.ini',
                                       conn_name=connection_name)

    Session = sessionmaker(bind=engine)
    session = Session()

    print('Defining table classes now...')

    class Legislators(Base):
        __tablename__ = 'legislators'
        legislator_id = Column(VARCHAR(250), index=True, primary_key=True)
        birthday = Column(DATE)
        gender = Column(VARCHAR(1))
        religion = Column(VARCHAR(250))
        first_name = Column(VARCHAR(250))
        last_name = Column(VARCHAR(250))
        party = Column(VARCHAR(250))

        social_accounts = relationship('Social')

    class Social(Base):
        __tablename__ = 'social'
        legislator_id = Column(VARCHAR(250), ForeignKey('legislators.legislator_id'), primary_key=True)
        facebook = Column(VARCHAR(250))
        twitter_screen_name = Column(VARCHAR(250))
        twitter_id = Column(VARCHAR(250))

        twitter_accounts = relationship('Profile_Log', foreign_keys=['id'],
                                        primaryjoin='user_profile_log.id == social.twitter_id')

    class Profile_Log(Base):
        __tablename__ = 'user_profile_log'
        id = Column(INTEGER, autoincrement=True)
        twitter_user_id = Column(VARCHAR(250))
        created_at = Column(DateTime)
        screen_name = Column(VARCHAR(250))
        description = Column(VARCHAR(250))
        location = Column(VARCHAR(250))
        favourites_count = Column(INTEGER)
        followers_count = Column(INTEGER)
        friends_count = Column(INTEGER)
        statuses_count = Column(INTEGER)
        profile_image_url = Column(VARCHAR(250))
        time_zone = Column(VARCHAR(200))
        time_collected = Column(DateTime)

        __table_args__ = (
            PrimaryKeyConstraint('screen_name', 'time_collected'),
            {},
        )

    class Tweets(Base):
        __tablename__ = 'tweets'
        id = Column(INTEGER, primary_key=True, autoincrement=True)
        tweet_id = Column(VARCHAR(30))
        twitter_screen_name = Column(VARCHAR(250))
        created_at = Column(DateTime)
        hashtags = Column(VARCHAR(300))
        text = Column(VARCHAR(500))
        favorite_count = Column(INTEGER)
        retweet_count = Column(INTEGER)
        followers_count = Column(INTEGER)
        lang = Column(VARCHAR)
        text_length = Column(INTEGER)
        media_type = Column(VARCHAR(200))
        user_mentions = Column(VARCHAR(250))
        time_collected = Column(DateTime)

    Base.metadata.create_all(engine)

    print('Transforming data for ingest now...')

    # Read in data
    legislators = pd.read_pickle('data/interim/current_legislators_df.pkl')
    social = pd.read_pickle('data/interim/legislators_social_df.pkl')
    user_profile_log = pd.read_pickle('data/interim/users_df.pkl')
    tweets = pd.read_pickle('data/interim/tweets_df.pkl')

    db_funcs.load_tweets_table(df=tweets, engine=engine, if_exists='replace')
    db_funcs.load_user_profile_table(df=user_profile_log, engine=engine, if_exists='replace')
    db_funcs.load_social_table(df=social, engine=engine, if_exists='replace')
    db_funcs.load_legislator_table(df=legislators, engine=engine, if_exists='replace')

    session.close_all()
    print('Database successfully created!')


if __name__ == '__main__':
    initial_data_gather()
    initial_data_load_db()
