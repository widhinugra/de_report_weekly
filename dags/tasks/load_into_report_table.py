
"""
This file contains all the operations needed to pull data
from table into report table.
Operations:
    - Use the keys and query table.
    - Pre process and clean the dataframe.
    - Insert data into the report
"""
import logging
import pandas as pd
from sqlalchemy import String, Integer, Float
from .db_utils import insert_into_table, get_df_from_db

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)


def populate_report_data(**kwargs):
    """
    This function is the driver function.
    Step 1: pull values from context
    Step 2 : query table.
    Step 3 : Pre process and clean the table
    Step 4 : Insert the data into the report table

    :param kwargs: context value holding the current_month and previous_month values
    :return: None
    """
    logging.info("Starting process to load data into report table")
    sql_query_data = get_staging_data()
    logging.info('Data fetched from table')
    df = preprocess_dataframe(sql_query_data)
    logging.info('Data cleaned and pre processed')
    insert_into_table('report', df, get_staging_dtypes())
    df.to_csv('~/data_files_airflow/report.csv')
    logging.info('Data loaded into report table')


def get_staging_data():
    """
    :return: dataframe from the sql query executed
    """
    sql_query = '''
select a.semester as semester_id, a.week as week_id, c2.name as course_name, (a.total/(b.total_day*c.total_student)::float)*100 as attendance_pct
from (
	select ca.schedule_id, s.course_id,  e.semester , (abs(s.start_dt - ca.attend_dt) / 7) + 1 as week, count(ca.student_id) as total
	from course_attendance ca 
	left join schedule s on s.id = ca.schedule_id 
	left join course c on c.id = s.course_id 
	left join enrollment e on e.student_id = ca.student_id and e.schedule_id = ca.schedule_id 
	group by ca.schedule_id , s.course_id,  e.semester , (abs(s.start_dt - ca.attend_dt) / 7) + 1
	order by ca.schedule_id , s.course_id,  e.semester , (abs(s.start_dt - ca.attend_dt) / 7) + 1
) a
left join (
	select id,course_id, count(*) as total_day
	from schedule s , 
	     unnest(string_to_array(s.course_days , ',')) as x(v) 
	where v::int > 0
	group by id, course_id 
	order by id, course_id
) b on b.id = a.schedule_id
left join  (
	select schedule_id, count(*) as total_student
	from enrollment e 
	group by schedule_id 
	order by schedule_id
) c on c.schedule_id = a.schedule_id
left join course c2 on c2.id =a.course_id
order by a.semester, a.course_id, a.week;'''

    return get_df_from_db(sql_query)


def preprocess_dataframe(sql_query_df):
    """

    :param sql_query_df: pandas dataframe
    :return: cleaned dataframe
    """
    df = pd.DataFrame(sql_query_df, columns=['semester_id', 'week_id', 'course_name', 'attendance_pct'])
    return df


def get_staging_dtypes():
    """
    :return: dt type of the popular_destination_history table
    """
    return {"semester_id": Integer(), "week_id": Integer(), "course_name": String(), "attendance_pct": Float()}

