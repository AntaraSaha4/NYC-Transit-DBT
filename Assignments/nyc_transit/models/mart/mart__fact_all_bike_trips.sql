SELECT
    started_at_ts,
    ended_at_ts,
    DATEDIFF('minute',started_at_ts,ended_at_ts) as duration_min,
    DATEDIFF('second',started_at_ts,ended_at_ts) as duration_sec,
    start_station_id,
    end_station_id
FROM {{ ref('stg__bike_data') }}