"""
This file contains the task to read the csv file and load the data into enrollment.

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


def perform_enrollment_etl(**kwargs):
    """
     This is the driver function to load data into the enrollment.
     Step 1: Load the data frame from file location
     Step 2: Clean and pre process the data frame
     Step 3: Insert the data frame into enrollment
     Step 4: Push the month field in dataframe into context.
             This value will be used in where clause of other tasks
    :param kwargs: push the month field into context to be used by other tasks
    :return: None
    """
    logging.info("Starting process to load data into staging")
    staging_df = load_dataframe()
    logging.info("Data frame loaded")
    preprocessed_df = preprocess_dataframe(staging_df)
    logging.info("Data frame cleaned and pre processed")

    insert_into_table('enrollment', preprocessed_df, get_staging_dtypes())
    logging.info("Data loaded into table")




def load_dataframe():
    """
    Read the csv file from location and return the pandas dataframe
    :return:  pandas data frame which will be loaded into DB
    """
    parse_dates = ['ENROLL_DT']
    return pd.read_csv('~/data_files_airflow/enrollment.csv',
                       usecols=["ID", "STUDENT_ID", "SCHEDULE_ID", "ACADEMIC_YEAR", "SEMESTER", "ENROLL_DT"], parse_dates=parse_dates)


def preprocess_dataframe(df):
    """
    :param df: pandas data frame which will be processed and cleaned
    :return: processed data frame which is to be loaded in DB
    """
    df.rename(columns={'ID': 'id', 'STUDENT_ID': 'student_id', 'SCHEDULE_ID':'schedule_id','ACADEMIC_YEAR':'academic_year','SEMESTER':'semester','ENROLL_DT':'enroll_dt'},inplace=True)
    df.id = df.id.astype(int)
    df.student_id = df.student_id.astype(int)
    df.schedule_id = df.schedule_id.astype(int)
    return df


def get_staging_dtypes():
    """
    :return: dt type which will be used to insert data into the DB
    """
    return {"id": Integer(), "student_id": Integer(), "schedule_id": Integer(), 
            "academic_year": String(), "semester": Integer(), "enroll_dt": Date()}