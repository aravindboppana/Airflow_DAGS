from airflow.operators import BashOperator,PythonOperator,HiveOperator

from airflow.models import DAG

from datetime import datetime, timedelta

default_args = {

'owner': 'airflow',

'start_date': datetime.now() -

timedelta(minutes=1),

'email': [],

'email_on_failure': False,

'email_on_retry': False,

'retries': 1,

'retry_delay': timedelta(minutes=5),

}

dag = DAG('queries_in_the_beginning_dag',

default_args=default_args, schedule_interval=None,

start_date=datetime.now() - timedelta(minutes=1))



start_metastore = BashOperator(

task_id='start_metastore',

bash_command="""nohup sqoop metastore &  """,

dag=dag)


create_database_in_hive = BashOperator(

task_id='create_database_in_hive',

bash_command="""hive -e "CREATE DATABASE IF NOT EXISTS hive" """,

dag=dag)


create_activitylog = BashOperator(

task_id='create_activitylog',

bash_command="""sqoop job --meta-connect jdbc:hsqldb:hsql://localhost:16000/sqoop --create hive.activitylog -- import --connect jdbc:mysql://localhost/hive --username root --password-file /user/cloudera/root_pwd.txt --table activitylog -m 2 --hive-import --hive-database hive --hive-table activitylog --incremental append --check-column id --last-value 0 """,

dag=dag)



start_metastore.set_downstream(create_database_in_hive)
create_database_in_hive.set_downstream(create_activitylog)

