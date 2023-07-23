SELECT
{{dbt_utils.star(ref('taxi+_zone_lookup')) }}
FROM {{ ref('taxi+_zone_lookup')}}