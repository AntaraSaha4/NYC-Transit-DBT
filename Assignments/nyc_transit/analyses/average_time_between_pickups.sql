-- depends_on: {{ ref('mart__fact_all_taxi_trips') }}
/*
1. SQL Query to find the average time between taxi pick ups per zone
    ● Use lead or lag to find the next trip per zone for each record
    ● then find the time difference between the pick up time for the current record
    and the next
    ● then use this result to calculate the average time between pick ups per zone.
2. Due to the query's extended execution time when applied to all zones, optimizations were necessary.
3. To reduce the execution time, I have implemented Jinja to iterate through zones individually and process the statements accordingly.
4. Alternatively, we can utilize the following SQL query to compute the average time between pick-ups for each zone.

Alternative SQL:

with pickup_times as (
    SELECT 
        m2.zone,
        pickup_datetime,
        LEAD(pickup_datetime,1) over (ORDER BY pickup_datetime) as next_pickup_time
    FROM 
        {{ ref('mart__fact_all_taxi_trips') }} m1
    INNER JOIN 
        {{ ref('mart__dim_locations') }} m2 
        ON m1.PUlocationID = m2.LocationID
		where m2.zone = 'Newark Airport'
)

SELECT
    zone,
    AVG(DATEDIFF('minute', pickup_datetime, next_pickup_time)) as avg_pickup_time_per_zone
FROM pickup_times
GROUP BY zone
*/

-- Create a temporary table to store the combined results
CREATE TEMP TABLE temp_combined_results (
  zone TEXT,
  avg_time_bw_pickup_mins DOUBLE
);

{% set zone_names = dbt_utils.get_column_values(ref('mart__dim_locations'), 'zone') %}

{% for zone_name in zone_names %}
    {% set sanitized_zone_name = zone_name.replace("'", "''") %} -- To handle the extra quote in the zone ('Governor's Island/Ellis Island/Liberty Island')
    
    INSERT INTO temp_combined_results
    select zone,avg(diff_pickup_time) as avg_time_bw_pickup_mins
    from 
    (
    SELECT 
        pickup_datetime AS current_trip_time,
        m2.zone,
        DATEDIFF('minute', pickup_datetime, LEAD(pickup_datetime, 1) OVER (ORDER BY pickup_datetime)) AS diff_pickup_time
    FROM 
        {{ ref('mart__fact_all_taxi_trips') }} m1
    INNER JOIN 
        {{ ref('mart__dim_locations') }} m2 
        ON m1.PUlocationID = m2.LocationID
        where m2.zone = '{{ sanitized_zone_name }}'
    )
    group by 1;
{% endfor %}

-- Select all rows from the temporary table to see the final results
SELECT * FROM temp_combined_results;
