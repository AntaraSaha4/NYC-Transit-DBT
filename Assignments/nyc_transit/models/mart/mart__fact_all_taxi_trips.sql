WITH trips_renamed AS
(
    SELECT 
        'fhv' as type,pickup_datetime,dropOff_datetime,PUlocationID,DOlocationID
        FROM {{ ref('stg__fhv_tripdata') }}
    UNION ALL
    SELECT 
        'fhvhv' as type,pickup_datetime,dropOff_datetime,PUlocationID,DOlocationID
        FROM {{ ref('stg__fhvhv_tripdata') }}
    UNION ALL
    SELECT 
        'green' as type,lpep_pickup_datetime,lpep_dropoff_datetime,PUlocationID,DOlocationID
        FROM {{ ref('stg__green_tripdata') }}
    UNION ALL
    SELECT 
        'yellow' as type,tpep_pickup_datetime,tpep_dropoff_datetime,PUlocationID,DOlocationID
        FROM {{ ref('stg__yellow_tripdata') }}
)

SELECT
    type,
    pickup_datetime,
    dropoff_datetime,
    DATEDIFF('minute',pickup_datetime,dropoff_datetime) as duration_min,
    DATEDIFF('second',pickup_datetime,dropoff_datetime) as duration_sec,
    pulocationid,
    dolocationid 
FROM trips_renamed