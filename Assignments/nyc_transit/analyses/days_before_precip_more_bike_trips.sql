/*
1. SQL query to determine if days immediately preceding precipitation or snow had more 
   bike trips on average than the days with precipitation or snow.
2. Only considering those days where data is available in both stg__central_park_weather table 
   and mart__fact_all_bike_trips tables. 
*/

with count_trips AS
( 
	SELECT 
		started_at_ts::date as date, 
		COUNT(1) as current_day_trips 
	FROM {{ ref('mart__fact_all_bike_trips') }} 
	GROUP BY 1
),
trips_count_comparison AS
(
SELECT W.date,PRCP,SNOW,CT.current_day_trips,LAG(CT.current_day_trips,1) OVER (ORDER BY W.date) as prev_day_trips
from {{ ref('stg__central_park_weather') }} W
INNER JOIN count_trips as CT
on CT.date = W.date
)
SELECT AVG(current_day_trips) as avg_trips_w_snw_prcp , AVG(prev_day_trips) as avg_trips_prev_w_snw_prcp 
from trips_count_comparison
where prcp>0 or snow>0