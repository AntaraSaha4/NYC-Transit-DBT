with source as (

    select * from {{ source('main', 'bike_data') }}

),

renamed as (

    select
        tripduration,
        starttime,
        stoptime,
        "start station id",
        "start station name",
        "start station latitude",
        "start station longitude",
        "end station id",
        "end station name",
        "end station latitude",
        "end station longitude",
        bikeid,
        usertype,
        "birth year",
        gender,
        ride_id,
        rideable_type,
        started_at,
        ended_at,
        start_station_name,
        start_station_id,
        end_station_name,
        end_station_id,
        start_lat,
        start_lng,
        end_lat,
        end_lng,
        member_casual,
        filename

    from source

)

select
	coalesce(starttime, started_at)::timestamp as started_at_ts,
	coalesce(stoptime, ended_at)::timestamp as ended_at_ts,
	coalesce(tripduration::int,datediff('second', started_at_ts, ended_at_ts)) tripduration,
	coalesce("start station id", start_station_id) as start_station_id,  
	coalesce("start station name", start_station_name) as start_station_name,
	coalesce("start station latitude", start_lat)::double as start_lat,
	coalesce("start station longitude", start_lng)::double as start_lng, 
	coalesce("end station id", end_station_id) as end_station_id,  
	coalesce("end station name", end_station_name) as end_station_name,
	coalesce("end station latitude", end_lat)::double as end_lat,
	coalesce("end station longitude", end_lng)::double as end_lng,
	filename
from renamed