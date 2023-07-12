/*
Code Documentation:
1. Standardizing column names with snake case.
2. Applying appropriate data types to columns:
    - PUlocationID and DOlocationID to integer as it contain value from 1 to 265.
3. Modified flags columns to Boolean for consistency with other flag columns using custom macro flag_to_boolean.
4. No column has been dropped.
5. Retaining records where :
    - Pickup_datetime is not after dropoff_datetime
    - Request_datetime is not after pickup_datetime
6. Did not remove duplicate records as this table does not have a defined primary key in the data dictionary.
7. Assumption:
    - Records with a trip miles and trip time of 0 are not erroneous; rather, they represent cancelled trips.
*/

with source AS(
    SELECT * FROM {{source('main', 'fhvhv_tripdata')}}
    ),

renamed AS (
    SELECT 
     hvfhs_license_num as TLC_hvs_license_id,
     dispatching_base_num as dispatching_base_id,
     originating_base_num as originating_base_id,
     request_datetime::TIMESTAMP as passenger_request_datatime,
     on_scene_datetime::TIMESTAMP as driver_arrival_datetime,
     pickup_datetime::TIMESTAMP as trip_pickup_datetime,
     dropoff_datetime::TIMESTAMP as trip_dropoff_datetime,
     PULocationID::integer as trip_start_location_id,
     DOLocationID::integer as trip_end_location_id,
     trip_miles::DOUBLE as trip_miles,
     trip_time::INTEGER as trip_duration_seconds,
     base_passenger_fare::DOUBLE as base_passenger_fare,
     tolls::DOUBLE as tolls_paid,
     bcf::DOUBLE as black_car_fund_amount,
     sales_tax::DOUBLE as sales_tax,
     congestion_surcharge::DOUBLE as congestion_surcharge,
     airport_fee::DOUBLE as airport_fee,
     tips::DOUBLE as tips_received,
     driver_pay::DOUBLE as driver_pay,
     {{flag_to_boolean('shared_request_flag')}}::BOOLEAN as shared_ride_agreement_flag,
     {{flag_to_boolean('shared_match_flag')}}::BOOLEAN as shared_ride_match_flag,
     {{flag_to_boolean('access_a_ride_flag')}}::BOOLEAN as MTA_trip_flag,
     {{flag_to_boolean('wav_request_flag')}}::BOOLEAN as wheelchair_accessible_request_flag,
     {{flag_to_boolean('wav_match_flag')}}::BOOLEAN as wheelchair_accessible_match_flag,
     filename as source_filename
     FROM source
     WHERE pickup_datetime <= dropoff_datetime
     AND request_datetime <= pickup_datetime
     )

select * from renamed
