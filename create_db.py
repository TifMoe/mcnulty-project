import pandas as pd
from configparser import ConfigParser
from sqlalchemy.dialects.postgresql import INTEGER, VARCHAR, DATE
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, ForeignKey, create_engine, MetaData

# Connect to database and instantiate session
Base = declarative_base()

config = ConfigParser()
config.read('config.ini')

engine = create_engine('postgresql://{}:{}@{}:{}/{}'
                       .format(config.get('AWS_RDS', 'user'),
                               config.get('AWS_RDS', 'password'),
                               config.get('AWS_RDS', 'host'),
                               config.get('AWS_RDS', 'port'),
                               config.get('AWS_RDS', 'db')))

Session = sessionmaker(bind=engine)
session = Session()


class Legislators(Base):
    __tablename__ = 'legislators'
    id = Column(VARCHAR(250), index=True, primary_key=True)
    birthday = Column(DATE)
    gender = Column(VARCHAR(1))
    religion = Column(VARCHAR(250))
    first_name = Column(VARCHAR(250))
    last_name = Column(VARCHAR(250))
    party = Column(VARCHAR(250))

    social_accounts = relationship('Social')


class Social(Base):
    __tablename__  = 'social'
    legislator_id = Column(VARCHAR(250), ForeignKey('legislators.id'), primary_key=True)
    facebook = Column(VARCHAR(250))
    twitter_name = Column(VARCHAR(250))
    twitter_id = Column(INTEGER, ForeignKey('twitter_profiles.id'))

    twitter_accounts = relationship('TwitterProfiles')


class TwitterProfiles(Base):
    __tablename__ = 'twitter_profiles'
    id = Column(INTEGER, primary_key=True)
    created_at = Column(DATE)
    name = Column(VARCHAR(250))
    description = Column(VARCHAR(250))
    location = Column(VARCHAR(250))
    favourites_count = Column(INTEGER)
    followers_count = Column(INTEGER)
    friends_count = Column(INTEGER)
    statuses_count = Column(INTEGER)


class Tweets(Base):
    __tablename__ = 'tweets'
    id = Column(INTEGER, primary_key=True)
    twitter_id = Column(INTEGER, ForeignKey('twitter_profiles.id'))
    created_at = Column(DATE)
    hashtags = Column(VARCHAR(250))
    text = Column(VARCHAR(250))
    favorite_count = Column(INTEGER)
    retweet_count = Column(INTEGER)
    followers_count = Column(INTEGER)

    twitter_accounts = relationship('TwitterProfiles')


# Tweets.__table__.drop(engine)
# Social.__table__.drop(engine)
# TwitterProfiles.__table__.drop(engine)
# Legislators.__table__.drop(engine)

Base.metadata.create_all(engine)


# Read in data and transform datatypes to make corresponding postgres columns
legislators = pd.read_pickle('data/current_legislators_df.pkl')
social = pd.read_pickle('data/legislators_social_df.pkl')
twitter_profiles = pd.read_pickle('data/twitter_profiles_df.pkl')
tweets = pd.read_pickle('data/tweets_df.pkl')
