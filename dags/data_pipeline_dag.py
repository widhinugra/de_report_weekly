"""
This dag file contains all the pipeline tasks to ingest, transform and push data into a database

DAG : agriaku_dag

Task 1: Check if the new csv file exists in the location.
Task 2: Extract, transform and Load the csv data.
Task 3: Read from the staging table, transform and insert data
        into the dwh_table.
Task 4: Read the latest data inserted into dwh_table and load into report table.

Data Flow :csv -> dwh_table-> report

"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from tasks.load_into_course import perform_course_etl
from tasks.load_into_schedule import perform_schedule_etl
from tasks.load_into_enrollment import perform_enrollment_etl
from tasks.load_into_course_attendance import perform_course_attendance_etl
from tasks.load_into_report_table import populate_report_data



# Default arguments with default parameters
default_args = {
    'owner': 'Ram',
    'start_date': datetime(2021, 9, 3),
    'retries': 2,
    'retry_delay': timedelta(seconds=60)
}

# creating DAG object and using it in all tasks within it
with DAG('agriaku_dag', default_args=default_args, schedule_interval='@monthly',
         template_searchpath=['/usr/local/airflow/sql_files'], catchup=True) as dag:


    start_job = DummyOperator(
                task_id='start',
                dag=dag
        )

    # Task to load the csv data into a staging table
    prepare_course_table = PythonOperator(task_id='insert_into_course_table',
                                           provide_context=True,
                                           python_callable=perform_course_etl)

    prepare_schedule_table = PythonOperator(task_id='insert_into_schedule_table',
                                           provide_context=True,
                                           python_callable=perform_schedule_etl)

    prepare_enrollment_table = PythonOperator(task_id='insert_into_enrollment_table',
                                           provide_context=True,
                                           python_callable=perform_enrollment_etl)

    prepare_course_attendance_table = PythonOperator(task_id='insert_into_course_attendance_table',
                                           provide_context=True,
                                           python_callable=perform_course_attendance_etl)

    prepare_report_table = PythonOperator(task_id='insert_into_report_table',
                                           python_callable=populate_report_data,
                                           provide_context=True)


    # Executing the tasks in order
    start_job >> [prepare_course_table, prepare_schedule_table, prepare_enrollment_table, prepare_course_attendance_table] >> prepare_report_table