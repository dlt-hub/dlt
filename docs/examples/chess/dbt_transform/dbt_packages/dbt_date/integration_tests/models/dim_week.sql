{{
    config(
        materialized = "table"
    )
}}
with periods_weeks as (
    {{ dbt_date.get_base_dates(n_dateparts=52, datepart="week") }}
)
select
    d.*
from
    periods_weeks d
