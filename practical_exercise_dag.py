from airflow.operators import BashOperator, PythonOperator, HiveOperator
from airflow.models import DAG
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime.now() - timedelta(minutes=1),
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
dag = DAG('practical_exercise_dag',
          default_args=default_args, schedule_interval=None,
          start_date=datetime.now() - timedelta(minutes=1))
load_data = BashOperator(
    task_id='load_data',
    bash_command="""python /home/cloudera/airflow/dags/practical_exercise_data_generator.py --load_data """,
    dag=dag)

create_csv = BashOperator(
    task_id='create_csv',
    bash_command="""
    cd /home/cloudera/airflow/dags/
    pwd
    python practical_exercise_data_generator.py --create_csv """,
    dag=dag)
user_import = BashOperator(
    task_id='user_import',
    bash_command="""sqoop import --connect jdbc:mysql://localhost/hive --username root --password-file /user/cloudera/root_pwd.txt --table user --m 2 --hive-import --hive-overwrite --hive-database hive --hive-table user """,
    dag=dag)
activitylog_import = BashOperator(
    task_id='activitylog_import',
    bash_command="""sqoop job --meta-connect jdbc:hsqldb:hsql://localhost:16000/sqoop --exec hive.activitylog """,
    dag=dag)
rename_csv = BashOperator(
    task_id='rename_csv',
    bash_command="""mv /home/cloudera/airflow/dags/user_upload_dump.*.csv /home/cloudera/airflow/dags/user_upload_dump.$(date +%s) """,
    dag=dag)
csv_into_hdfs = BashOperator(
    task_id='csv_into_hdfs',
    bash_command="""hadoop fs -put $(ls -t | head -1) /user/cloudera/csv_files """,
    dag=dag)
create_user_report = BashOperator(
    task_id='create_user_report',
    bash_command="""hive -e "CREATE TABLE IF NOT EXISTS hive.user_report(user_id int,total_updates int,total_inserts int,total_deletes int,last_activity_type string,is_active string,upload_count int)" """,
    dag=dag)
load_user_report = BashOperator(
    task_id='load_user_report',
    bash_command="""hive -e "INSERT OVERWRITE TABLE hive.user_report select t9.id,CASE WHEN t2.total_updates is null THEN '0' ELSE t2.total_updates END,CASE WHEN t1.total_inserts is null THEN '0' ELSE t1.total_inserts END,CASE WHEN t3.total_deletes is null THEN '0' ELSE t3.total_deletes END,t7.type,CASE WHEN t8.is_active is null THEN 'FALSE' ELSE t8.is_active END,CASE WHEN t5.upload_count is null THEN '0' ELSE t5.upload_count END from (select user_id, count(*) as total_inserts from hive.activitylog where type='INSERT' group by user_id) as t1 FULL JOIN(select user_id, count(*) as total_updates from hive.activitylog where type='UPDATE' group by user_id) as t2 on t1.user_id = t2.user_id FULL JOIN(select user_id, count(*) as total_deletes from hive.activitylog where type='DELETE' group by user_id) as t3 on t1.user_id=t3.user_id FULL JOIN(SELECT user_id, count(user_id) as upload_count FROM hive.csv_table GROUP by user_id) as t5 on t1.user_id=t5.user_id LEFT JOIN(select activitylog.user_id,activitylog.timestamp,activitylog.type from hive.activitylog JOIN(select user_id,MAX(timestamp) as timestamp from hive.activitylog group by user_id) AS t6 ON activitylog.timestamp=t6.timestamp and activitylog.user_id=t6.user_id) as t7 on t7.user_id=t1.user_id LEFT JOIN(SELECT user_id,timestamp,CASE WHEN timestamp > timestamp-172800 THEN 'Active' ELSE 'InActive' END AS is_active from hive.activitylog) as t8 on t1.user_id=t8.user_id and t8.timestamp=t7.timestamp RIGHT JOIN (select id from hive.user) as t9 ON t1.user_id=t9.id" """,
    dag=dag)
create_user_total = BashOperator(
    task_id='create_user_total',
    bash_command="""hive -e "CREATE TABLE IF NOT EXISTS hive.user_total(time_ran string,total_users int,users_added int)" """,
    dag=dag)
load_user_total = BashOperator(
    task_id='load_user_total',
    bash_command="""max_value=$(hive -e "select CASE WHEN max(total_users) is null THEN '0' ELSE max(total_users) END from hive.user_total")
      set -- $max_value
      value=$1
      hive -e "INSERT INTO table hive.user_total select current_timestamp as time_ran,count(id) as total_users,count(id)-$value as users_added from hive.user"  """,
    dag=dag)
create_external_table_csv = BashOperator(
    task_id='create_external_table_csv',
    bash_command="""hive -e "CREATE EXTERNAL TABLE IF NOT EXISTS hive.csv_table (user_id int, file_type string,mytime bigint) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/user/cloudera/csv_files' TBLPROPERTIES ('skip.header.line.count"="1')" """,
    dag=dag)
load_data.set_downstream(user_import)
load_data.set_downstream(activitylog_import)
user_import.set_downstream(create_user_report)
activitylog_import.set_downstream(create_user_report)
user_import.set_downstream(create_user_total)
activitylog_import.set_downstream(create_user_total)
create_user_report.set_downstream(load_user_report)
create_user_total.set_downstream(load_user_total)
create_csv.set_downstream(create_external_table_csv)
create_external_table_csv.set_downstream(rename_csv)
rename_csv.set_downstream(csv_into_hdfs)
