"""
This file contains common DB util methods used in the tasks.
"""

import configparser
import os
import pandas as pd

from sqlalchemy import create_engine


def get_db_engine():
    """
    This function is used to create DB engine to connect to DB
    :return: DB engine to execute queries
    """
    config = configparser.ConfigParser()
    path = '/'.join((os.path.abspath(__file__).replace('\\', '/')).split('/')[:-1])
    config.read(os.path.join(path, 'etl_datapipeline.cfg'))
    return create_engine('{}://{}:{}@{}:{}/{}'.format(*config['DB_CONFIG'].values()))


def insert_into_table(table_name, df, df_dtypes):
    """
    This function is used to insert a data frame which the corresponding dtypes into table
    :param table_name: table name to which data is inserted
    :param df: dataframe to be loaded into DB
    :param df_dtypes: data type of the columns
    :return: None
    """
    engine = get_db_engine()
    connection = engine.connect()
    df.to_sql(table_name, connection, if_exists='append', index=False, dtype=df_dtypes)
    connection.close()

def get_df_from_db(sql_query):
    """

    :param sql_query: Execute query to fetch data from DB into dataframe
    :return: pandas data frame
    """
    engine = get_db_engine()
    return pd.read_sql_query(sql_query, engine)
