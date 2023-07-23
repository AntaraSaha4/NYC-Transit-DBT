WITH trips_renamed AS
(
    SELECT 'bike' as type,started_at_ts,ended_at_ts FROM {{ ref('stg__bike_data') }}
    UNION ALL
    SELECT 'fhv' as type,pickup_datetime,dropOff_datetime FROM {{ ref('stg__fhv_tripdata') }}
    UNION ALL
    SELECT 'fhvhv' as type,pickup_datetime,dropOff_datetime FROM {{ ref('stg__fhvhv_tripdata') }}
    UNION ALL
    SELECT 'green' as type,lpep_pickup_datetime,lpep_dropoff_datetime FROM {{ ref('stg__green_tripdata') }}
    UNION ALL
    SELECT 'yellow' as type,tpep_pickup_datetime,tpep_dropoff_datetime FROM {{ ref('stg__yellow_tripdata') }}
)

SELECT
    type,
    started_at_ts,
    ended_at_ts,
    DATEDIFF('minute',started_at_ts,ended_at_ts) as duration_min,
    DATEDIFF('second',started_at_ts,ended_at_ts) as duration_sec
FROM trips_renamed