-- choose role and create WH
use role sysadmin;
use warehouse compute_wh;

-- create database
create or replace database aqi_project;
use database AQI_PROJECT;
-- we are using public schema

-- create an internal stage
create or replace stage landing_zone;
show stages;

-- create file format
create or replace file format json_format
type = 'JSON';
show file formats;

-- load 1st sample json file
-- resource-folder/02-raw-data/full_aqi_sample_11hr.json

-- list the stage location
list @LANDING_ZONE;

-- query the internal stage json file using $ notation
select t.$1 
from @AQI_PROJECT.public.LANDING_ZONE (file_format => json_format) t;

-- query the root.records[1].elements using $ and DOT notation
select 
    t.$1:total::int as record_count,
    t.$1:count::int as count,

    -- from records key
    t.$1:records[1].last_update::text as record_ts,

    -- station detail
    t.$1:records[1].country::text as country,
    t.$1:records[1].state::text as state,
    t.$1:records[1].city::text as city,
    t.$1:records[1].station::text as station,
    t.$1:records[1].latitude::number(12,7) as latitude,
    t.$1:records[1].longitude::number(12,7) as longitude,

    -- pollutant details
    t.$1:records[1].pollutant_id::text as pollutant_id,
    t.$1:records[1].pollutant_min::text as pollutant_min,
    t.$1:records[1].pollutant_max::text as pollutant_max,
    t.$1:records[1].pollutant_avg::text as pollutant_avg   
from @AQI_PROJECT.public.LANDING_ZONE (file_format => json_format) t;

-- CLEANUP
-- Step 1: Drop any objects created for this project

-- Drop file format
DROP FILE FORMAT IF EXISTS aqi_project.public.json_format;

-- Drop stage
DROP STAGE IF EXISTS aqi_project.public.landing_zone;

-- Drop database (this removes all schemas, tables, file formats, stages, etc. under it)
DROP DATABASE IF EXISTS aqi_project;

-- Step 4: Verify cleanup
SHOW DATABASES LIKE 'AQI_PROJECT';
SHOW STAGES;
SHOW FILE FORMATS;
SHOW WAREHOUSES LIKE 'COMPUTE_WH';
