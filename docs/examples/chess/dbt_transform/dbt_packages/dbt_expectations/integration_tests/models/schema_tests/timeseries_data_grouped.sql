with dates as (
    select * from {{ ref('timeseries_base') }}
),
groupings as (
    select * from {{ ref('series_4') }}
),
row_values as (
    select * from {{ ref('series_10') }}
),
add_row_values as (

    select
        cast(dates.date_day as {{ dbt_expectations.type_datetime() }}) as date_day,
        cast(dates.date_day as {{ dbt_expectations.type_timestamp() }}) as date_timestamp,
        cast(groupings.generated_number as {{ dbt.type_int() }}) as group_id,
        cast(100 * abs({{ dbt_expectations.rand() }}) as {{ dbt.type_float() }}) as row_value
    from
        dates
        cross join groupings
        cross join row_values

),
add_logs as (

    select
        *,
        {{ dbt_expectations.log_natural('nullif(row_value, 0)') }} as row_value_log
    from
        add_row_values
)
select *
from
    add_logs
