-- change the context
use role sysadmin;
use schema aqi_project_db.clean_sch;
use warehouse adhoc_wh;

-- checking data
select * from aqi_project_db.stage_sch.raw_aqi order by id;

-- we are going with index-record-ts as pk
select 
    id, index_record_ts
from 
    aqi_project_db.stage_sch.raw_aqi 
where 
    index_record_ts is not null
order by id;

-- final columns + JSON data which ive not selcted here
select 
    id,
    index_record_ts,
    record_count,
    json_version,
    _stg_file_name,
    _stg_file_load_ts,
    _stg_file_md5 ,
    _copy_data_ts
from 
    aqi_project_db.stage_sch.raw_aqi 
where 
    index_record_ts is not null; -- this will give all 24 records

-- loaded some duplicate data files to check if it takes duplicates
select 
    id,
    index_record_ts,
    record_count,
    json_version,
    _stg_file_name,
    _stg_file_load_ts,
    _stg_file_md5 ,
    _copy_data_ts,
    row_number() over (partition by index_record_ts order by _stg_file_load_ts desc) as latest_file_rank
from 
    aqi_project_db.stage_sch.raw_aqi 
where 
    index_record_ts is not null; -- this will give all 24 records

-- ok it did show duplicatse
-- i will select first row after doing row num by partioning over index-record-ts 
-- because in this case index-record-ts is the ts of the data recorded by weather tower probably

-- de-duplication of the records + flattening it 
-- (first it was saved in a json now we removed them - keys as oolumns)
with air_quality_with_rank as (
    select 
        index_record_ts,
        json_data,
        record_count,
        json_version,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5 ,
        _copy_data_ts,
        row_number() over (partition by index_record_ts order by _stg_file_load_ts desc) as latest_file_rank
    from aqi_project_db.stage_sch.raw_aqi
    where index_record_ts is not null
),
unique_air_quality_data as (
    select 
        * 
    from 
        air_quality_with_rank 
    where latest_file_rank = 1
)
    select 
        index_record_ts,
        hourly_rec.value:country::text as country,
        hourly_rec.value:state::text as state,
        hourly_rec.value:city::text as city,
        hourly_rec.value:station::text as station,
        hourly_rec.value:latitude::number(12,7) as latitude,
        hourly_rec.value:longitude::number(12,7) as longitude,
        hourly_rec.value:pollutant_id::text as pollutant_id,
        hourly_rec.value:pollutant_max::text as pollutant_max,
        hourly_rec.value:pollutant_min::text as pollutant_min,
        hourly_rec.value:pollutant_avg::text as pollutant_avg,

        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
  from 
    unique_air_quality_data ,
    lateral flatten (input => json_data:records) hourly_rec;

-- okay great prac now we create table for this

-- why dynamic table now - 
    -- i created DT because they automatically refresh and maintain cleaned, transformed views 
    -- of raw data without manual ETL runs, auto refresh in short based on target_lag, in this
    -- case it is downstream system

-- creating dynamic table
create or replace dynamic table clean_aqi_dt
    target_lag='downstream'
    warehouse=transform_wh
as
  with air_quality_with_rank as (
    select 
        index_record_ts,
        json_data,
        record_count,
        json_version,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5 ,
        _copy_data_ts,
        row_number() over (partition by index_record_ts order by _stg_file_load_ts desc) as latest_file_rank
    from aqi_project_db.stage_sch.raw_aqi
    where index_record_ts is not null
),
unique_air_quality_data as (
    select 
        * 
    from 
        air_quality_with_rank 
    where latest_file_rank = 1
)
    select 
        index_record_ts,
        hourly_rec.value:country::text as country,
        hourly_rec.value:state::text as state,
        hourly_rec.value:city::text as city,
        hourly_rec.value:station::text as station,
        hourly_rec.value:latitude::number(12,7) as latitude,
        hourly_rec.value:longitude::number(12,7) as longitude,
        hourly_rec.value:pollutant_id::text as pollutant_id,
        hourly_rec.value:pollutant_max::text as pollutant_max,
        hourly_rec.value:pollutant_min::text as pollutant_min,
        hourly_rec.value:pollutant_avg::text as pollutant_avg,

        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
  from 
    unique_air_quality_data ,
    lateral flatten (input => json_data:records) hourly_rec;


select * from clean_aqi_dt limit 10;