{{ config(post_hook='insert into {{this}}(staff_id) VALUES (-1)') }}

with staff_base as (
select
*,
(case when active::int = 1 then 1 else 0 end) as "active_int",
(case when active::int = 1 then 'yes' else 'no' end) as "active_desc",
'{{ run_started_at.strftime ("%Y-%m-%d %H:%M:%S")}}'::timestamp as dbt_time
from
staff
)
select
	staff_id,
	first_name,
	last_name,
	email,
  	active_int as active,
  	active_desc,
	last_update,
  	dbt_time
from
	staff_base
			