from datetime import datetime
import time
import numpy as np
from sqlalchemy import create_engine
from configparser import ConfigParser
from sqlalchemy import Table, MetaData
from sqlalchemy.orm import sessionmaker

from src.data.db_functions import db_create_engine


def drop_tables(table_list, engine):
    for table_name in table_list:
        eval(table_name).__table__.drop(engine)
    return True


def drop_all_tables():
    tables = ['Social', 'Legislators', 'Profile_Log', 'Tweets']
    drop_tables(tables, engine=db_create_engine(config_file='config.ini',
                                                conn_name='PostgresConfig'))


"""
engine = db_create_engine('config.ini', 'PostgresConfig')
metadata = MetaData(bind=engine)
Session = sessionmaker(bind=engine)
session = Session()

# Drop rows from tweets table to correct UTC issue
tweets_table = Table('tweets', metadata, autoload=True)
session.query(tweets_table).filter(tweets_table.c.time_collected > '2018-02-17 17:43:36.38051').\
    delete(synchronize_session=False)

# Drop rows from users table to correct UTC issue
users_table = Table('user_profile_log', metadata, autoload=True)
session.query(users_table).filter(users_table.c.time_collected > '2018-02-17 17:43:36.38051').\
    delete(synchronize_session=False)

session.commit()
session.close()
"""

