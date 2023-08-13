{{config(materialized='table',post_hook="insert into {{this}}(customer_id) VALUES (-1)")}}

SELECT
customer_id ,
first_name ,
last_name ,
{{concat_it('first_name' , 'last_name')}} as the_full_name
from {{ ref('Hello_world') }}

where 1=1
and customer_id < {{var('cust_id')}}