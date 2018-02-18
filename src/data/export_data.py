import gspread
from gspread.exceptions import RequestError
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from src.data.db_functions import db_create_engine
from src.data.sql_queries import legislators_sql, tweets_sql


def create_gs_client(gs_credentials_json):
    # Initialize connection to google sheets
    scope = ['https://spreadsheets.google.com/feeds']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(gs_credentials_json, scope)

    gc = gspread.authorize(credentials)
    return gc


def next_available_row(worksheet):
    str_list = list(filter(None, worksheet.col_values(1)))
    return str(len(str_list)+1)


def add_new_rows(df, sheet, first_blank_row, gs_client):
    next_row = first_blank_row

    for index, row in df.iterrows():

        range_build = 'A' + str(next_row) + ':K' + str(next_row)
        cell_list = sheet.range(range_build)

        try:
            for i in range(0, len(row)):
                cell_list[i].value = row[i]

            sheet.update_cells(cell_list)
            next_row += 1

        except RequestError:

            # Refresh authorization
            gs_client.login()

            for i in range(0, len(row)):
                cell_list[i].value = row[i]

            sheet.update_cells(cell_list)
            next_row += 1


def refresh_tableau_csv_files():
    # Connect to aws and read legislator summary
    engine = db_create_engine(config_file='config.ini', conn_name='PostgresConfig')
    legislator_summary = pd.read_sql(sql=legislators_sql, con=engine)
    all_tweets = pd.read_sql(sql=tweets_sql, con=engine)

    # Write data to csv
    legislator_summary.to_csv('data/csv-tableau-source/legislator_summary.csv', index=False)
    all_tweets.to_csv('data/csv-tableau-source/tweet_data.csv', index=False)
