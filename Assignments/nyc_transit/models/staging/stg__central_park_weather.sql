with source as (

    select * from {{ source('main', 'central_park_weather') }}

),

renamed as (

    select
        station,
        name,
        date::date as date,
        awnd::double as awnd,
        prcp::double as prcp,
        snow::double as snow,
        snwd::double as snwd,
        tmax::int as tmax,
        tmin::int as tmin,
        filename

    from source

)

select * from renamed