The DAGs are to perform the practical_exercise task.

In the two DAGs queries_in_the_beginning_dag.py is to start the metastore to import data into hive.It also creates a database in hive if it doesn't exist.

Practical_exercise_dag.py is to kick the jobs that are required to import data into hive and to give the results in the reporting tables.

To generate user_total table a shell script is used.Below are the queries that are in the script. 

    max_value=$(hive -e "select CASE WHEN max(total_users) is null THEN '0' ELSE max(total_users) END from hive.user_total")
    set -- $max_value
    value=$1
    hive -e "INSERT INTO table hive.user_total select current_timestamp as time_ran,count(id) as total_users,count(id)-$value as users_added from hive.user"
