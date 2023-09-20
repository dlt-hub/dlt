{{
    config(
        materialized = "table"
    )
}}
with periods_hours as (
    {{ dbt_date.get_base_dates(n_dateparts=24*28, datepart="hour") }}
)
select
    d.*
from
    periods_hours d
