"""
This file contains the task to read the csv file and load the data into course.

Operations:
    - Read csv file from file location.
    - Pre process and clean the data in the data frame.
    - insert the processed data frame into DB.

"""

import logging
from statistics import mode

import pandas as pd
from sqlalchemy import String, Integer

from .db_utils import insert_into_table

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)


def perform_course_etl(**kwargs):
    """
     This is the driver function to load data into the course.
     Step 1: Load the data frame from file location
     Step 2: Clean and pre process the data frame
     Step 3: Insert the data frame into course
    :return: None
    """
    logging.info("Starting process to load data into course")
    staging_df = load_dataframe()
    logging.info("Data frame loaded")
    preprocessed_df = preprocess_dataframe(staging_df)
    logging.info("Data frame cleaned and pre processed")

    insert_into_table('course', preprocessed_df, get_staging_dtypes())
    logging.info("Data loaded into table")


def load_dataframe():
    """
    Read the csv file from location and return the pandas dataframe
    :return:  pandas data frame which will be loaded into DB
    """
    return pd.read_csv('~/data_files_airflow/course.csv',
                       usecols=["ID", "NAME"])


def preprocess_dataframe(df):
    """
    :param df: pandas data frame which will be processed and cleaned
    :return: processed data frame which is to be loaded in DB
    """
    df.rename(columns={'ID': 'id', 'NAME': 'name'},inplace=True)
    df.id = df.id.astype(int)
    df.name = df.name.astype(str)
    return df


def get_staging_dtypes():
    """
    :return: dt type which will be used to insert data into the DB
    """
    return {"id": Integer(), "name": String()}
