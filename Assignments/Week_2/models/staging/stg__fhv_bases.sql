/*
Code Documentation:
1. Standardizing column names with snake case.
2. No change to the data type.
3. Dropping useless column 'dba' due to null values for 80% of records.
4. No duplicate or bad records found.
*/

with source AS(
    SELECT * FROM {{source('main', 'fhv_bases')}}
    ),

renamed AS (
    SELECT 
     base_number as base_id,
     base_name,
     dba_category as doing_business_as_category,
     filename as source_filename
     FROM source
)

select * from renamed
