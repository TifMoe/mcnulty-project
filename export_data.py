import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from db_functions import db_create_engine
from sql_queries import legislators_sql, tweets_sql

# Initialize connection to google sheets
scope = ['https://spreadsheets.google.com/feeds']
credentials = ServiceAccountCredentials.from_json_keyfile_name('google_service_account.json', scope)

gc = gspread.authorize(credentials)
sheet = gc.open("Legislator Twitter Data").sheet1

# Connect to aws and read legislator summary
engine = db_create_engine(config_file='config.ini', conn_name='PostgresConfig')
legislator_summary = pd.read_sql(sql=legislators_sql, con=engine)
all_tweets = pd.read_sql(sql=tweets_sql, con=engine)

# Write data to csv
legislator_summary.to_csv('data/legislator_summary.csv', index=False)
all_tweets.to_csv('data/tweet_data.csv', index=False)

# Write data out to Google Sheet
for row in legislator_summary.iterrows():
    sheet.append_row(row[1].tolist())
