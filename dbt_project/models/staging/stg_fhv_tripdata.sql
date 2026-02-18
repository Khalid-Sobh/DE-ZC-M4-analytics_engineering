with source as (
    select * from {{ source('raw', 'fhv_2019') }}
),
filtered as (

  select *
  from source
  where dispatching_base_num is not null -- Filter requirement
)
select
    -- Rename fields to project naming conventions
    dispatching_base_num,
    pickup_datetime,
    dropoff_datetime,
    cast(pulocationid as integer) as pickup_location_id,
    cast(dolocationid as integer) as dropoff_location_id,
    sr_flag,
    affiliated_base_number
from filtered