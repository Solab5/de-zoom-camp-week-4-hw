{{ config(materialized='table') }}

with trips_data as (
    select * from {{ ref('fact_trips') }}
),

filtered_data as (
    select 
        service_type,
        fare_amount,
        extract(year from pickup_datetime) as year,
        extract(month from pickup_datetime) as month
    from trips_data
    where fare_amount > 0
      and trip_distance > 0
      and payment_type_description in ('Cash', 'Credit Card')
),

percentiles as (
    select
        service_type,
        year,
        month,
        PERCENTILE_CONT(fare_amount, 0.97) OVER (PARTITION BY service_type, year, month) as p97,
        PERCENTILE_CONT(fare_amount, 0.95) OVER (PARTITION BY service_type, year, month) as p95,
        PERCENTILE_CONT(fare_amount, 0.90) OVER (PARTITION BY service_type, year, month) as p90
    from filtered_data
)

select distinct service_type, year, month, 
       round(p97, 1) as p97, 
       round(p95, 1) as p95, 
       round(p90, 1) as p90
from percentiles
where year = 2020 and month = 4
order by service_type
