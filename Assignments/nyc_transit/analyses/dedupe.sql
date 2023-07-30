/*
1. SQL Quert to Use Window functions with QUALIFY and ROW_NUMBER to remove duplicate rows.
2. Prefer rows with a later time stamp
*/

SELECT * 
FROM {{ ref('events') }}
qualify 
row_number()
OVER (PARTITION BY event_id
ORDER BY insert_timestamp DESC)
= 1
ORDER BY event_id