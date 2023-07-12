/*
Code Documentation:
1. Standardizing column names with snake case.
2. Applying appropriate data types to columns:
    - Converted date column to Date type
    - Converted precipitation, snow depth, snowfall, max/min temperature, avg wind speed columns to double.
    - Converted trips, station_in_service to integer.
    - converted day of week to tinyint.
3. Dropped column year,month as this can be extracted from the observation date column.
4. No duplicate or bad records found.
*/

with source AS(
    SELECT * FROM {{source('main', 'daily_citi_bike_trip_counts_and_weather')}}
    ),
renamed AS (
    SELECT
    date::date as observation_date,     
    trips::integer as total_trips,
    precipitation::DOUBLE as precipitation,
    snow_depth::DOUBLE as snow_depth_inch,
    snowfall::DOUBLE as snow_fall_inch,
    max_temperature::DOUBLE as max_temperature,
    min_temperature::DOUBLE as min_temperature,
    try_cast(average_wind_speed as DOUBLE) as average_wind_speed,
    dow::tinyint as day_of_week,
    holiday::BOOLEAN as holiday_flag,
    try_cast(stations_in_service as integer) as stations_in_service,
    weekday::BOOLEAN as weekday_flag,
    weekday_non_holiday::BOOLEAN as weekday_non_holiday_flag,
    filename as source_filename
    FROM source
)

select * from renamed