/*
1. SQL to calculate the number of trips and average duration by borough and zone
2. Only the trips with a valid PUlocationID found in the mart__dim_locations table are considered.
*/

SELECT 
    m2.borough, 
    m2.Zone,
    count(1) as total_trips,
    avg(m1.duration_min) as avg_trip_duration_mins,
    avg(m1.duration_sec) as avg_trip_duration_secs
FROM {{ ref('mart__fact_all_taxi_trips')}} m1
INNER JOIN {{ ref('mart__dim_locations')}} m2 
ON m1.PUlocationID = m2.LocationID
GROUP BY 1,2