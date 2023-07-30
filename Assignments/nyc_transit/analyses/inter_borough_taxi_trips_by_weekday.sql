-- naive JOIN strategy
with all_trips as
(select 
    weekday(pickup_datetime) as weekday, 
    count(*) trips
    from {{ ref('mart__fact_all_taxi_trips') }} t
    group by all),

inter_borough as
(select 
    weekday(pickup_datetime) as weekday, 
    count(*) as trips
from {{ ref('mart__fact_all_taxi_trips') }} t
join {{ ref('mart__dim_locations') }} pl on t.PUlocationID = pl.LocationID
join {{ ref('mart__dim_locations') }} dl on t.DOlocationID = dl.LocationID
where pl.borough != dl.borough
group by all)

select all_trips.weekday,
       all_trips.trips as all_trips,
       inter_borough.trips as inter_borough_trips,
       inter_borough.trips / all_trips.trips as percent_inter_borough
from all_trips
join inter_borough on (all_trips.weekday = inter_borough.weekday);