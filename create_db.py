from sqlalchemy.dialects.postgresql import INTEGER, VARCHAR, DATE
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, ForeignKey
from db_connect import db_create_engine


Base = declarative_base()
engine = db_create_engine(config_file='config.ini',
                          conn_name='AWS_RDS')


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


Base.metadata.create_all(engine)

# To drop tables uncomment and run below
"""
Tweets.__table__.drop(engine)
Social.__table__.drop(engine)
TwitterProfiles.__table__.drop(engine)
Legislators.__table__.drop(engine)
"""
