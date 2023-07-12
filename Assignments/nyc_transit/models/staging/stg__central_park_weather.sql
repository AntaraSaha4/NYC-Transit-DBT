/*
Code Documentation:
1. Standardizing column names with snake case.
2. Applying appropriate data types to columns:
    - Converted date column to Date type
    - Converted precipitation, snow depth, snowfall, max/min temperature, avg wind speed columns to double.
3. No column has been dropped.
4. No duplicate or bad records found.
*/

with source AS(
    SELECT * FROM {{source('main', 'central_park_weather')}}
    ),

renamed AS (
    SELECT
    STATION as station_id,
    NAME as station_name,
    DATE::date as observation_date,
    try_cast(AWND as DOUBLE) as average_wind_speed,    
    PRCP::DOUBLE as precipitation,
    SNOW::DOUBLE as snow_fall_inch,
    SNWD::DOUBLE as snow_depth_inch,
    TMAX::DOUBLE as max_temperature,
    TMIN::DOUBLE as min_temperature,
    filename as source_filename 
    FROM source
)

select * from renamed