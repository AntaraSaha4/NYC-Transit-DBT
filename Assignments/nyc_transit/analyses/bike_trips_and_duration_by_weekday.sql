/*
SQL Query to count & total time of bikes trips by weekday
*/

SELECT
    weekday(started_at_ts) as weekday,
    count(1) as total_trips,
    SUM(duration_sec) as total_trip_duration_secs
FROM {{ ref('mart__fact_all_bike_trips') }}
GROUP BY 1