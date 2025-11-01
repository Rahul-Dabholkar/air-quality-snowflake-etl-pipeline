-- change context
use role sysadmin;
use schema aqi_project_db.stage_sch;
use warehouse adhoc_wh;


-- create an internal stage and enable directory service
create stage if not exists raw_stg
directory = ( enable = true)
comment = 'all the air quality raw data will store in this internal stage location';


 -- create file format to process the JSON file
create file format if not exists json_file_format     
  type = 'JSON'
  compression = 'AUTO' 
  comment = 'this is json file format object';


show stages;
list @raw_stg;

-- load the data that has been downloaded manually
-- run the list command to check it
select 
    * 
from 
    @aqi_project_db.stage_sch.raw_stg
    (file_format => JSON_FILE_FORMAT) t;

-- JSON file analysis using json editor
select 
    Try_TO_TIMESTAMP(t.$1:records[0].last_update::text, 'dd-mm-yyyy hh24:mi:ss') as index_record_ts,
    t.$1:total::int as record_count,
    t.$1:version::text as json_version,
    -- meta data information
    metadata$filename as _stg_file_name,
    metadata$FILE_LAST_MODIFIED as _stg_file_load_ts,
    metadata$FILE_CONTENT_KEY as _stg_file_md5,
    current_timestamp() as _copy_data_ts

from @aqi_project_db.stage_sch.raw_stg
(file_format => JSON_FILE_FORMAT) t;

-- metadata properties and why are we saving it
    -- we save metadata of file for better traceability, 
    -- and automation by describing when, where, 
    -- and how data was ingested
  
-- creating a raw table to have air quality data
create or replace transient table raw_aqi (
    id int primary key autoincrement,
    index_record_ts timestamp not null,
    json_data variant not null,
    record_count number not null default 0,
    json_version text not null,
    -- audit columns for debugging
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp()
);
-- describe table raw_aqi;

-- why transient table and not permanent table
    -- I am using a transient table for staging since  it’s cheaper (but no Fail-safe)
    -- also we should only use permanent table for critical, long-term data (Fail-safe period(7 days) + Backup)
    -- thats why it also costs more

-- copy command
-- copy command implemented as a task
create or replace task copy_air_quality_data
    warehouse = load_wh
    schedule = 'USING CRON 0 * * * * Asia/Kolkata'
as
copy into raw_aqi (index_record_ts,json_data,record_count,json_version,_stg_file_name,_stg_file_load_ts,_stg_file_md5,_copy_data_ts) from 
(
    select 
        Try_TO_TIMESTAMP(t.$1:records[0].last_update::text, 'dd-mm-yyyy hh24:mi:ss') as index_record_ts,
        t.$1,
        t.$1:total::int as record_count,
        t.$1:version::text as json_version,
        metadata$filename as _stg_file_name,
        metadata$FILE_LAST_MODIFIED as _stg_file_load_ts,
        metadata$FILE_CONTENT_KEY as _stg_file_md5,
        current_timestamp() as _copy_data_ts
            
   from @aqi_project_db.stage_sch.raw_stg as t
)
file_format = (format_name = 'aqi_project_db.stage_sch.JSON_FILE_FORMAT') 
ON_ERROR = ABORT_STATEMENT; 

show tasks;

-- I will first manually run just the copy command to check the data 
-- then I will create the task for it which can run every day

-- found a way to enable task using query. this is only possible by accadmin
use role accountadmin;
grant execute task, execute managed task on account to role sysadmin;
use role sysadmin;
alter task aqi_project_db.stage_sch.copy_air_quality_data resume;

-- check the data
select *
from raw_aqi
limit 10;

-- checking files for duplicates
select 
    index_record_ts,record_count,json_version,_stg_file_name,_stg_file_load_ts,_stg_file_md5 ,_copy_data_ts,
    row_number() over (partition by index_record_ts order by _stg_file_load_ts desc) as latest_file_rank
from raw_aqi 
order by index_record_ts desc
limit 10;