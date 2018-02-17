# mcnulty-project

### Project 3 at [Metis](www.thisismetis.com)
**Objective**: Build a classification model to predict political affiliation of elected officials using twitter data

**Tools**:
- Gather data using Twitter's API
- Build Postgres Relational Database
- Build Flask app to construct database using CLI command

## Setup

#### 1) Clone this repo
```bash
$ git clone https://github.com/TifMoe/mcnulty-project.git
```

#### 2) In your cloned repo, clone current legislators YAML database from [this repository](https://github.com/unitedstates/congress-legislators) to create a 'congress-legislators' subdirectory
```bash
mcnulty-project$ git clone https://github.com/unitedstates/congress-legislators.git
```

#### 3) Add a file named 'config.ini' to the root directory of your cloned mcnulty-project
```
# Example 'config.ini' file

[TwitterKeys]
consumer_key=your_consumer_key_goes_here
consumer_secret=your_consumer_secret_goes_here
access_token=your_access_token_goes_here
access_token_secret=your_access_token_secret_goes_here

[PostgresConfig]
user=your_postgres_username_goes_here
password=your_postgres_password_goes_here
host=your_postgre_shost_goes_here
port=your_postgres_port_goes_here (probably 5432)
db=your_postgres_database_name_goes_here
```
If you need to get a twitter API key, you can get one by signing up for an app [here](https://apps.twitter.com/app/new)

*Note: You must label the twitter API section of your config file **TwitterKeys** as displayed above*

## Create Database

#### 1) Navigate to root directory of cloned repo in your command line to install requirements
```bash
$ pip install -r requirements.txt
```

#### 2) Export the flask app to fetch twitter data and populate database
```bash
$ export FLASK_APP=/yourfilepathtoclonedrepo/mcnulty-project/src/data/db_app.py

$ flask initial_data_gather # This will fetch twitter data from past 30 days for all available legislators in YAML db

$ flask initial_data_load_db # This will create a Postgres database with legislator twitter data
```
*Note: Please allow up to 40 minutes for the **initial_data_gather** and **initial_data_load_db** commands to fetch and transform the twitter data then define appropriate table schema and load data to your postgres db.*


