from sqlalchemy import create_engine
from configparser import ConfigParser


def db_create_engine(config_file, conn_name):
    """
    Create a sqlAlchemy engine to connect to Postgres database given some connection parameters in config file.
    Note - this can be used to connect to any Postgres db either remotely or locally

    :param config_file: A config file with connection configuration details under conn_name heading
    :param conn_name: The section name for set of configuration details for desired connection
    :return: A sqlAlchemy engine connected to aws postgres database
    """
    config = ConfigParser()
    config.read(config_file)

    engine = create_engine('postgresql://{}:{}@{}:{}/{}'
                           .format(config.get(conn_name, 'user'),
                                   config.get(conn_name, 'password'),
                                   config.get(conn_name, 'host'),
                                   config.get(conn_name, 'port'),
                                   config.get(conn_name, 'db')))

    return engine
