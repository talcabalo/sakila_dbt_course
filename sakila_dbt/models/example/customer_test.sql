{{config(materialized='table', alias = 'customers_alias', schema = 'new')}}

SELECT
*
from {{ ref('Hello_world') }}

where 1=1
and customer_id < {{var('cust_id')}}