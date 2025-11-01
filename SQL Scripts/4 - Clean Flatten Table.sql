use role sysadmin;
use schema aqi_project_db.clean_sch;
use warehouse adhoc_wh;

-- So currently our problem is  - we have exploded the json data keys in columns
-- but since it was an array , now we have duplicate rows with only a single columns
-- data changing, in this case we are getting different pollutant id for same index_record_ts row

-- so to solve this I will transpose this table on pollutant id 
-- to get all the values on that column in a single row

-- First lets checkdata
select 
    hour(index_record_ts) as measurement_hours,
    * 
from 
    clean_aqi_dt 
where 
    country = 'India' and
    state = 'Delhi' and 
    station = 'Mundka, Delhi - DPCC'
    -- Mundka, Delhi - DPCC
    -- IGI Airport (T3), Delhi - IMD
order by 
    index_record_ts, id;

-- i have also realised that there are NULL columns with no data, since we dont have any DS usecase
-- so we will be just replacing them with 0 without consideration, 
-- @todo find another solution for null if use case changes

-- let transpose single row and check
select 
    index_record_ts,
    country,
    state,
    city,
    station,
    latitude,
    longitude,
    max(case when pollutant_id = 'PM2.5' then pollutant_avg end) as pm25_avg,
    max(case when pollutant_id = 'PM10' then pollutant_avg end) as pm10_avg,
    max(case when pollutant_id = 'SO2' then pollutant_avg end) as so2_avg,
    max(case when pollutant_id = 'NO2' then pollutant_avg end) as no2_avg,
    max(case when pollutant_id = 'NH3' then pollutant_avg end) as nh3_avg,
    max(case when pollutant_id = 'CO' then pollutant_avg end) as co_avg,
    max(case when pollutant_id = 'OZONE' then pollutant_avg end) as o3_avg
from 
    clean_aqi_dt
where 
    country = 'India' and
    state = 'Karnataka' and 
    station = 'Silk Board, Bengaluru - KSPCB' and 
    index_record_ts = '2024-03-01 11:00:00.000'
 group by 
    index_record_ts, country, state, city, station, latitude, longitude
    order by country, state, city, station;


-- ok i need to take care of null in combined query since i will have to create another tocheck so this is better

create or replace dynamic table clean_flatten_aqi_dt
    target_lag='30 min'
    warehouse=transform_wh
as
-- first lets run this with limit and check data 
with step01_combine_pollutant_cte as (
    SELECT 
        INDEX_RECORD_TS,
        COUNTRY,
        STATE,
        CITY,
        STATION,
        LATITUDE,
        LONGITUDE,
        MAX(CASE WHEN POLLUTANT_ID = 'PM10' THEN POLLUTANT_AVG END) AS PM10_AVG,
        MAX(CASE WHEN POLLUTANT_ID = 'PM2.5' THEN POLLUTANT_AVG END) AS PM25_AVG,
        MAX(CASE WHEN POLLUTANT_ID = 'SO2' THEN POLLUTANT_AVG END) AS SO2_AVG,
        MAX(CASE WHEN POLLUTANT_ID = 'NO2' THEN POLLUTANT_AVG END) AS NO2_AVG,
        MAX(CASE WHEN POLLUTANT_ID = 'NH3' THEN POLLUTANT_AVG END) AS NH3_AVG,
        MAX(CASE WHEN POLLUTANT_ID = 'CO' THEN POLLUTANT_AVG END) AS CO_AVG,
        MAX(CASE WHEN POLLUTANT_ID = 'OZONE' THEN POLLUTANT_AVG END) AS O3_AVG
    FROM 
        clean_aqi_dt
    group by 
        index_record_ts, country, state, city, station, latitude, longitude
        order by country, state, city, station
),
step02_replace_na_cte as (
    select 
        INDEX_RECORD_TS,
        COUNTRY,
        replace(STATE,'_',' ') as STATE,
        CITY,
        STATION,
        LATITUDE,
        LONGITUDE,
        CASE 
            WHEN PM25_AVG = 'NA' THEN 0 
            WHEN PM25_AVG is Null THEN 0 
            ELSE round(PM25_AVG)
        END as PM25_AVG,
        CASE 
            WHEN PM10_AVG = 'NA' THEN 0 
            WHEN PM10_AVG is Null THEN 0 
            ELSE round(PM10_AVG)
        END as PM10_AVG,
        CASE 
            WHEN SO2_AVG = 'NA' THEN 0 
            WHEN SO2_AVG is Null THEN 0 
            ELSE round(SO2_AVG)
        END as SO2_AVG,
        CASE 
            WHEN NO2_AVG = 'NA' THEN 0 
            WHEN NO2_AVG is Null THEN 0 
            ELSE round(NO2_AVG)
        END as NO2_AVG,
         CASE 
            WHEN NH3_AVG = 'NA' THEN 0 
            WHEN NH3_AVG is Null THEN 0 
            ELSE round(NH3_AVG)
        END as NH3_AVG,
         CASE 
            WHEN CO_AVG = 'NA' THEN 0 
            WHEN CO_AVG is Null THEN 0 
            ELSE round(CO_AVG)
        END as CO_AVG,
         CASE 
            WHEN O3_AVG = 'NA' THEN 0 
            WHEN O3_AVG is Null THEN 0 
            ELSE round(O3_AVG)
        END as O3_AVG,
    from step01_combine_pollutant_cte
)
select *,
from step02_replace_na_cte;

-- great now we create table. we have so far
    -- step 1 - saved raw json + metadata + handle duplicates
    -- step 2 - exploded all json data 
    -- step 3 - transposed all json data + handle null values 

select *
from AQI_PROJECT_DB.CLEAN_SCH.CLEAN_FLATTEN_AQI_DT
WHERE 
    country = 'India' and
    state = 'Karnataka' and 
    station = 'Silk Board, Bengaluru - KSPCB';