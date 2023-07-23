/*
SQL Query to find count of total trips, trips starting and ending in a different borough, and 
percentage w/ different start/end by weekday.
*/

SELECT 
    WEEKDAY(M1.pickup_datetime) as Weekday,
    count(1) as total_trips,
    sum(CASE WHEN T1.Borough != T2.Borough THEN 1 ELSE 0 END) AS diff_total_trips,
    (sum(CASE WHEN T1.Borough != T2.Borough THEN 1 ELSE 0 END)/sum(1))*100 AS percent_diff_start_end
FROM {{ ref('mart__fact_all_taxi_trips') }} M1
LEFT JOIN {{ ref('mart__dim_locations') }} T1
ON M1.pulocationid = T1.LocationID
LEFT JOIN {{ ref('mart__dim_locations') }} T2
ON M1.dolocationid = T2.LocationID
GROUP BY 1