{{config(materialized='incremental',unique_key='payment_id') }}
select * ,
'{{ run_started_at.strftime ("%Y-%m-%d %H:%M:%S")}}' as dbt_time
from payment 