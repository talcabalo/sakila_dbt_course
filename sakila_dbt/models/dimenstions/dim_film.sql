{{ config(materialized='incremental',unique_key='film_id',post_hook='insert into {{this}}(film_id) VALUES (-1)') }}

with film_0 as (
	select
	*,
	(case
	when length<=75 then 'short'
	when (length>75 and length<=120) then 'medium'
	when length>120 then 'long'
	else 'na' end) as length_desc,
	COALESCE(language_id,0) as original_language_id_zero,
	case when POSITION('Trailers' in special_features::varchar)>0 then 1 else 0 end  as has_trailers,
	case when POSITION('Commentaries' in special_features::varchar)>0 then 1 else 0 end  as has_commentaries,
	case when POSITION('Deleted Scenes' in special_features::varchar)>0 then 1 else 0 end  as has_deleted_scenes,
	case when POSITION('Behind the Scenes' in special_features::varchar)>0 then 1 else 0 end  as has_behind_the_scenes,
	'{{ run_started_at.strftime ("%Y-%m-%d %H:%M:%S")}}'::timestamp as dbt_time
	from film),
    language as (
	select * from language),
    category as (
	select * from category),
    film_category as (
	select * from film_category),
    
    film_1 as (
	select
	film_0.*,
	language.name as lang_name
	from film_0 left join language on 1=1
	and film_0.language_id = language.language_id)
    
    select * from film_1