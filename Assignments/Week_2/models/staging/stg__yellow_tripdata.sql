/*
Code Documentation:
1. Standardizing column names with snake case.
2. Applying appropriate data types to columns:
    - PUlocationID and DOlocationID to integer as it contain value from 1 to 265.
    - passenger_count to integer as it will never be in fraction.
    - VendorID,RatecodeID,payment_type and trip_type to tinyint based on column values
3. Modified flags columns to Boolean for consistency with other flag columns using custom macro flag_to_boolean.
4. No column has been dropped.
5. Retaining records where :
    - tpep_pickup_datetime is not after tpep_dropoff_datetime
6. Assumption:
    - Records with trip distance of 0 are not erroneous; rather, they represent cancelled trips.
    - Due to limited information, RateCodeID values that are null or equal to 99 are not considered erroneous; instead, 
      they indicate special trips or Negotiated Fare.
7. Did not remove duplicate records as this table does not have a defined primary key in the data dictionary.
*/

with source AS(
    SELECT * FROM {{source('main', 'yellow_tripdata')}}
    ),

renamed AS (
    SELECT 
    VendorID::tinyint as vendor_id,
    tpep_pickup_datetime::TIMESTAMP as trip_pickup_datetime,
    tpep_dropoff_datetime::TIMESTAMP as trip_dropoff_datetime,
    passenger_count::integer as passenger_count,
    trip_distance::DOUBLE as trip_distance,
    RatecodeID::tinyint as rate_code_id,
    {{flag_to_boolean('store_and_fwd_flag')}}::BOOLEAN as store_and_fwd_flag,
    PULocationID::integer as trip_start_location_id,
    DOLocationID::integer as trip_end_location_id,
    payment_type::tinyint as payment_type,
    fare_amount::DOUBLE as fare_amount,
    extra::DOUBLE as additiona_charges,
    mta_tax::DOUBLE as mta_tax,
    tip_amount::DOUBLE as tip_amount,
    tolls_amount::DOUBLE as tolls_amount,
    improvement_surcharge::DOUBLE as improvement_surcharge,
    total_amount::DOUBLE as total_amount,
    congestion_surcharge::DOUBLE as congestion_surcharge,
    airport_fee::DOUBLE as airport_fee,
    filename as source_filename
     FROM source
    WHERE tpep_pickup_datetime <= tpep_dropoff_datetime
)

select * from renamed
