/*
1. SQL query to calculate the 7 day moving min, max, avg, sum for precipitation
and snow for every day in the weather data, defining the window only once.
2. The 7 day window should center on the day in question.
*/

SELECT
    date,
    min(snow) over seven_days as min_snow,
    max(snow) over seven_days as max_snow,
    avg(snow) over seven_days as avg_snow,
    sum(snow) over seven_days as total_snow,
    min(prcp) over seven_days as min_prcp,    
    max(prcp) over seven_days as max_prcp,
    avg(prcp) over seven_days as avg_prcp,
    sum(prcp) over seven_days as total_prcp
FROM {{ ref('stg__central_park_weather') }}
WINDOW seven_days AS (
    ORDER BY DATE
    RANGE BETWEEN INTERVAL 3 DAYS PRECEDING AND
                  INTERVAL 3 DAYS FOLLOWING
)