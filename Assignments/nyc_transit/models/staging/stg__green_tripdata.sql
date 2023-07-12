with source as (

    select * from {{ source('main', 'green_tripdata') }}

),

renamed as (

    select
        vendorid,
        lpep_pickup_datetime,
        lpep_dropoff_datetime,
        {{flag_to_bool("store_and_fwd_flag")}} as store_and_fwd_flag,        ratecodeid,
        pulocationid,
        dolocationid,
        passenger_count,
        trip_distance,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        ehail_fee,
        improvement_surcharge,
        total_amount,
        payment_type,
        trip_type,
        congestion_surcharge,
        filename

    from source

)

select * from renamed