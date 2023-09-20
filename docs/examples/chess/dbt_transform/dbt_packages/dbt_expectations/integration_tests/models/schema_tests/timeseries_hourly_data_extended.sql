with dates as (

    select * from {{ ref('timeseries_hourly') }}

),
row_values as (
    select * from {{ ref('series_10') }}
),
add_row_values as (

    select
        cast(dates.date_hour as {{ dbt_expectations.type_datetime() }}) as date_hour,
        cast(100 * abs({{ dbt_expectations.rand() }}) as {{ dbt.type_float() }}) as row_value

    from
        dates
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
