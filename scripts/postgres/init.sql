CREATE TABLE airflow__extra_conf(
  conf_name  VARCHAR (255) PRIMARY KEY,
  conf_value VARCHAR (255) NOT NULL
);

CREATE TABLE IF NOT EXISTS course(id int, name varchar (100));
CREATE TABLE IF NOT EXISTS schedule(id int, course_id int, lecture_id int, start_dt date, end_dt date, course_days varchar(100));
CREATE TABLE IF NOT EXISTS enrollment(id int, student_id int, schedule_id int, academic_year varchar(100), semester int, enroll_dt date);
CREATE TABLE IF NOT EXISTS course_attendance(id int, student_id int, schedule_id int, attend_dt date);
CREATE TABLE IF NOT EXISTS report(semester_id int, week_id int, course_name varchar(100), attendance_pct float);
