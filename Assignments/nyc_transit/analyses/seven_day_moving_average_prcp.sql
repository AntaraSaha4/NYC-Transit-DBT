/*
1. SQL query to calculate the 7 day moving average precipitation for every day in
the weather data.
2. The 7 day window should center on the day in question.
3. While computing the 7-day moving average, if any data is missing for a day within the window, 
   it is designated as NULL instead of zero. Consequently, the final calculated value for the moving average will be NULL, 
   reflecting the unavailability of sufficient data to produce a valid result.
*/

SELECT prcp,date, 
    (
    LAG(prcp,1) over (order by date)+ 
    LAG(prcp,2) over (order by date) +
    LAG(prcp,3) over (order by date) +
    prcp+
    LEAD(prcp,1) over (order by date)+
    LEAD(prcp,2) over (order by date)+
    LEAD(prcp,3) over (order by date)
    )/7 as moving_avg_prcp
from {{ ref('stg__central_park_weather') }}
order by date;