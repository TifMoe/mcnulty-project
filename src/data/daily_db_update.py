import pandas as pd
from src.data.sql_queries import last_updated
from src.data.db_functions import db_create_engine


legislators = pd.read_pickle('data/interim/current_legislators_df.pkl')
social = pd.read_pickle('data/interim/legislators_social_df.pkl')

# Find latest update
engine = db_create_engine(config_file='config.ini', conn_name='PostgresConfig')
last_updated = pd.read_sql_query(sql=last_updated, con=engine)
last_updated_time = last_updated.iloc[0, 0]