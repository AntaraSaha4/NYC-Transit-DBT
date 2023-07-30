/*
1. SQL query query to compare an individual fare to the zone, borough and overall
average fare using the fare_amount in yellow taxi trip data.
2. Only the trips with a valid PUlocationID found in the mart__dim_locations table are considered.
*/

SELECT 
  s1.fare_amount,
  AVG(s1.fare_amount) OVER(PARTITION BY m1.zone) AS avg_zone_fare,
  AVG(s1.fare_amount) OVER(PARTITION BY m1.borough) AS avg_borough_fare,
  AVG(fare_amount) OVER() AS overall_average_fare
FROM 
  {{ ref('stg__yellow_tripdata') }} s1
INNER JOIN 
  {{ ref('mart__dim_locations')}} m1 
ON s1.PULocationID = m1.LocationID
