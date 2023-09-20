{{
    config(
        materialized = "table"
    )
}}
with fp as (
    {{ dbt_date.get_fiscal_periods(ref('dates'), year_end_month=1, week_start_day=1) }}
)
select
    f.*
from
    fp f
