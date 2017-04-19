max_value=$(hive -e "select CASE WHEN max(total_users) is null THEN '0' ELSE max(total_users) END from hive.user_total")
set -- $max_value
value=$1
hive -e "INSERT INTO table hive.user_total select current_timestamp as time_ran,count(id) as total_users,count(id)-$value as users_added from hive.user"
