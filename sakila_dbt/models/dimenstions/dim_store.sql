{{ config(post_hook='insert into {{this}}(store_id) VALUES (-1)') }}

with stg_store as (
		select
    *,
    '{{ run_started_at.strftime ("%Y-%m-%d %H:%M:%S")}}'::timestamp as dbt_time
     from store
),
staff as (
 select * from {{ ref('dim_staff') }}
),
address as (
  select * from address
),
city as (
  select * from city
),
country as (
  select * from country
),
stg_store_1 as (-- add staff
		select
		stg_store.*,
		staff.first_name as staff_first_name,
		staff.last_name as staff_last_name
		from
		stg_store
		left join staff  on 1=1
		and stg_store.manager_staff_id = staff.staff_id
),
stg_store_2 as (-- add adress
		select
		stg_store_1.*,
		address.address,
		city.city_id,
		city.city,
		country.country_id,
		country.country
		from
		stg_store_1

		left join address on 1=1
		and stg_store_1.address_id =address.address_id

		left join city on 1=1
		and address.city_id = city.city_id

		left join country on  1=1
		and city.country_id = country.country_id
)
select
  store_id,
  manager_staff_id,
  staff_first_name,
  staff_last_name,
  address_id,
  address,
  city_id,
  city,
  country_id,
  country,
  last_update,
  dbt_time
from stg_store_2

