"""
This file contains the task to read the csv file and load the data into schedule.

Operations:
    - Read csv file from file location.
    - Pre process and clean the data in the data frame.
    - insert the processed data frame into DB.

"""

import logging
from statistics import mode

import pandas as pd
from sqlalchemy import String, Integer, Date

from .db_utils import insert_into_table

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)


def perform_schedule_etl(**kwargs):
    """
     This is the driver function to load data into the schedule.
     Step 1: Load the data frame from file location
     Step 2: Clean and pre process the data frame
     Step 3: Insert the data frame into schedule
    :return: None
    """
    logging.info("Starting process to load data into staging")
    staging_df = load_dataframe()
    logging.info("Data frame loaded")
    preprocessed_df = preprocess_dataframe(staging_df)
    logging.info("Data frame cleaned and pre processed")
    logging.info(preprocessed_df)

    insert_into_table('schedule', preprocessed_df, get_staging_dtypes())
    logging.info("Data loaded into table")



def load_dataframe():
    """
    Read the csv file from location and return the pandas dataframe
    :return:  pandas data frame which will be loaded into DB
    """
    parse_dates = ['START_DT','END_DT']
    return pd.read_csv('~/data_files_airflow/schedule.csv',
                       usecols=["ID", "COURSE_ID", "LECTURER_ID", "START_DT", "END_DT", "COURSE_DAYS"], parse_dates=parse_dates)


def preprocess_dataframe(df):
    """
    :param df: pandas data frame which will be processed and cleaned
    :return: processed data frame which is to be loaded in DB
    """
    df.rename(columns={'ID': 'id', 'COURSE_ID': 'course_id', 'LECTURER_ID':'lecture_id','START_DT':'start_dt','END_DT':'end_dt','COURSE_DAYS':'course_days'},inplace=True)
    df.id = df.id.astype(int)
    df.course_id = df.course_id.astype(int)
    df.lecture_id = df.lecture_id.astype(int)
    return df


def get_staging_dtypes():
    """
    :return: dt type which will be used to insert data into the DB
    """
    return {"id": Integer(), "course_id": Integer(), "lecture_id": Integer(), 
            "start_dt": Date(), "end_dt": Date(), "course_days": String()}
