/*
SQL Query to find total number of trips ending in service_zones 'Airports' or 'EWR'
*/

SELECT 
    count(1) as total_trips 
FROM {{ ref('mart__fact_all_taxi_trips') }} M
INNER JOIN {{ ref('mart__dim_locations') }} T
    on M.dolocationid = T.LocationID
WHERE T.service_zone in ('Airports','EWR')