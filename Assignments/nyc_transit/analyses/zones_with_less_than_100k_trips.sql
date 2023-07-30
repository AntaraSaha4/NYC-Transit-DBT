/*
1. SQL query to finds all the Zones where there are less than 100000 trips.
2. Only the trips with a valid PUlocationID found in the mart__dim_locations table are considered.
*/

SELECT 
    m2.Zone,
    count(1) as total_trips
FROM {{ ref('mart__fact_all_taxi_trips')}} m1
INNER JOIN {{ ref('mart__dim_locations')}} m2 
ON m1.PUlocationID = m2.LocationID
GROUP BY 1
having total_trips < 100000