find_where_local_date_greater_than =
select *
from jsr310_column_sqlserver
where local_date > ?

find_where_local_date_time_greater_than =
select *
from jsr310_column_sqlserver
where local_date_time > ?