{{ config(materialized='table') }}

with trips_data as (
    select * from {{ ref('fact_trips') }}
),

quarterly_revenue as (
    select 
        extract(year from pickup_datetime) as year_number,
        extract(quarter from pickup_datetime) as quarter_number,
        service_type,
        sum(total_amount) as total_revenue
    from trips_data
    group by 1, 2, 3
),

with_previous_year as (
    select
        current_year.year_number,
        current_year.quarter_number,
        current_year.service_type,
        current_year.total_revenue as quarterly_revenue,
        prev_year.total_revenue as prev_year_revenue
    from quarterly_revenue current_year
    left join quarterly_revenue prev_year
        on current_year.quarter_number = prev_year.quarter_number
        and current_year.year_number = prev_year.year_number + 1
)

select
    year_number,
    quarter_number,
    service_type,
    quarterly_revenue,
    prev_year_revenue,
    quarterly_revenue - prev_year_revenue as revenue_change,
    case 
        when prev_year_revenue is null or prev_year_revenue = 0 
        then null
        else round(100.0 * (quarterly_revenue - prev_year_revenue) / prev_year_revenue, 2)
    end as yoy_growth_percentage
from with_previous_year
order by year_number, quarter_number