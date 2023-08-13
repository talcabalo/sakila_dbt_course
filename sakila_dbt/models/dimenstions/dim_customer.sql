{{ config(materialized='incremental',unique_key='customer_id',post_hook='insert into {{this}}(customer_id) VALUES (-1)') }}

with customer_base as (

select
 *,
	concat(customer.first_name,' ',customer.last_name) as full_name,
	substring(email from POSITION('@' in email)+1 for char_length(email)-POSITION('@' in email)) as domain,
	customer.active::int as active_int,
	(case when customer.active = 0 then 'no' else 'yes' end)::varchar(100) as "active_desc",
  '{{ run_started_at.strftime ("%Y-%m-%d %H:%M:%S")}}'::timestamp as dbt_time
from customer),
address as (
    select * from address
),
city as (
    select * from city
),
country as (
    select * from country
)
 select 
  customer_base.customer_id,
  customer_base.store_id,
  customer_base.first_name,
  customer_base.last_name,
  customer_base.full_name,
  customer_base.domain,
  customer_base.email,
  customer_base.active_int as active,
  customer_base.active_desc,

  address.address_id,
  address.address,
  city.city_id,
  city.city,
  country.country_id,
  country.country,

  customer_base.create_date,
  customer_base.last_update,
  customer_base.dbt_time

  from
  customer_base

left join address on 1=1
	and customer_base.address_id =address.address_id

	left join city on 1=1
	and address.city_id = city.city_id

  left join country on 1=1
  and country.country_id = city.country_id

  where 1=1

  {% if is_incremental() %}
  and customer_base.last_update::timestamp > (select max(last_update) from {{this}})
  {% endif %}