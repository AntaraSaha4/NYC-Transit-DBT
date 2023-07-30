/*
1. SQL query which finds taxi trips which don’t have a pick up location_id in the
locations table.
*/

SELECT 
    m1.* 
FROM {{ ref('mart__fact_all_taxi_trips')}} m1
LEFT JOIN {{ ref('mart__dim_locations')}} m2 
ON m1.PUlocationID = m2.LocationID
WHERE m2.LocationID is null
order by m1.pickup_datetime