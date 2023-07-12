/*
Code Documentation:
1. Standardizing column names with snake case.
2. Applying appropriate data types to columns:
    - PUlocationID and DOlocationID to integer as it contain value from 1 to 265.
    - passenger_count to integer as it will never be in fraction.
    - VendorID, RatecodeID, payment_type and trip_type to tinyint based on column values
3. Modified flags columns to Boolean for consistency with other flag columns using custom macro flag_to_boolean.
4. Dropping useless column 'ehail_fee' due to null values for all of records.
5. Retaining records where :
    - lpep_pickup_datetime is not after lpep_dropoff_datetime
6. Assumption:
    - Records with a trip distance of 0 are not erroneous; rather, they represent cancelled trips.
    - There are ~33% records with blank passesnger count and payment type.Since it is not clear why they are blank, hence keeping them for now. 
7. Did not remove duplicate records as this table does not have a defined primary key in the data dictionary.
*/

with source AS(
    SELECT * FROM {{source('main', 'green_tripdata')}}
    ),

renamed AS (
    SELECT 
    VendorID::tinyint as vendor_id,
    lpep_pickup_datetime::TIMESTAMP as trip_pickup_datetime,
    lpep_dropoff_datetime::TIMESTAMP as trip_dropoff_datetime,
    {{flag_to_boolean('store_and_fwd_flag')}}::BOOLEAN as store_and_fwd_flag,
    RatecodeID::tinyint as rate_code_id,
    PULocationID::integer as trip_start_location_id,
    DOLocationID::integer as trip_end_location_id,
    passenger_count::integer as passenger_count,
    trip_distance::DOUBLE as trip_distance,
    fare_amount::DOUBLE as fare_amount,
    extra::DOUBLE as additiona_charges,
    mta_tax::DOUBLE as mta_tax,
    tip_amount::DOUBLE as tip_amount,
    tolls_amount::DOUBLE as tolls_amount,
    improvement_surcharge::DOUBLE as improvement_surcharge,
    total_amount::DOUBLE as tolls_amount,    
    payment_type::tinyint as payment_type,
    trip_type::tinyint as trip_type,    
    congestion_surcharge::DOUBLE as congestion_surcharge,
    filename as source_filename
    FROM source
    WHERE lpep_pickup_datetime <= lpep_dropoff_datetime
)

select * from renamed
