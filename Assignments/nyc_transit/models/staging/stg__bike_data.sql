/*
Code Documentation:
1. Standardizing column names with snake case.
2. Applying appropriate data types to columns:
    - Start and End station id, latitude, and longitude to double
    - Start and End time to date time
    - Bike Id, Trip duration, and birth year to integer
3. Merging columns like start/end station id, start/end station name, start/end station longitude/latitude, start/end date columns with old and new data format.
4. Also for gender column instead of 1,2,0 changes it to Male,Female,Unknown based on Data Dictionay.
3. No column has been dropped.
4. Retaining records where :
    - Ride start time is not after the Ride end time
5. Did not remove duplicate records as this table does not have a defined primary key in the data dictionary.
*/

with source AS(
    SELECT * FROM {{source('main', 'bike_data')}}
    ),

renamed AS (
    SELECT
    ride_id,
    bikeid::integer as bike_id,
    rideable_type, 
    COALESCE(starttime,started_at)[:19]::timestamp as ride_start_time,
    TRY_CAST(COALESCE("start station id",start_station_id) as DOUBLE) as start_station_id,
    COALESCE("start station name",start_station_name) as start_station_name,
    COALESCE("start station latitude",start_lat)::DOUBLE as start_station_latitude,
    COALESCE("start station longitude",start_lng)::DOUBLE as start_station_longitude,
    COALESCE(stoptime,ended_at)[:19]::timestamp as ride_end_time,
    TRY_CAST(COALESCE("end station id",end_station_id) as DOUBLE) as end_station_id,
    COALESCE("end station name",end_station_name) as end_station_name,
    COALESCE("end station latitude",end_lat)::DOUBLE as end_station_latitude,
    COALESCE("end station longitude",end_lng)::DOUBLE as end_station_longitude,
    tripduration::integer as trip_duration_seconds,    
    usertype as user_type,
    "birth year"::integer as birth_year,
    CASE gender
           WHEN '1' THEN 'Male'
           WHEN '2' THEN 'Female'
           WHEN '0' THEN 'Unknown'
       END AS gender,
    member_casual,
    filename as source_filename 
    FROM source
    WHERE COALESCE(starttime,started_at)[:19]::timestamp <= COALESCE(stoptime,ended_at)[:19]::timestamp
)

select * from renamed