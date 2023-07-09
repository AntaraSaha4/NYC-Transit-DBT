/*
Code Documentation:
1. Standardizing column names with snake case.
2. Applying appropriate data types to columns:
    - PUlocationID and DOlocationID to integer as it contain value from 1 to 265.
    - Converting datetime columns to timestamp data type.
3. Dropping useless column 'SRFlag' due to null values for all of records.
4. Did not remove duplicate records as this table does not have a defined primary key in the data dictionary.
*/


with source AS(
    SELECT * FROM {{source('main', 'fhv_tripdata')}}
    ),

renamed AS (
    SELECT 
     dispatching_base_num as dispatching_base_id,
     pickup_datetime::TIMESTAMP as trip_pickup_datetime,
     dropOff_datetime::TIMESTAMP as trip_dropoff_datetime,
     PUlocationID::integer as trip_start_location_id,
     DOlocationID::integer as trip_end_location_id,
     Affiliated_base_number AS affiliated_base_id,
     filename as source_filename
     FROM source
)

select * from renamed
