{% macro get_date_dimension(start_date, end_date) %}
    {{ adapter.dispatch('get_date_dimension', 'dbt_date') (start_date, end_date) }}
{% endmacro %}

{% macro default__get_date_dimension(start_date, end_date) %}
with base_dates as (
    {{ dbt_date.get_base_dates(start_date, end_date) }}
),
dates_with_prior_year_dates as (

    select
        cast(d.date_day as date) as date_day,
        cast({{ dbt.dateadd('year', -1 , 'd.date_day') }} as date) as prior_year_date_day,
        cast({{ dbt.dateadd('day', -364 , 'd.date_day') }} as date) as prior_year_over_year_date_day
    from
    	base_dates d

)
select
    d.date_day,
    {{ dbt_date.yesterday('d.date_day') }} as prior_date_day,
    {{ dbt_date.tomorrow('d.date_day') }} as next_date_day,
    d.prior_year_date_day as prior_year_date_day,
    d.prior_year_over_year_date_day,
    {{ dbt_date.day_of_week('d.date_day', isoweek=false) }} as day_of_week,
    {{ dbt_date.day_of_week('d.date_day', isoweek=true) }} as day_of_week_iso,
    {{ dbt_date.day_name('d.date_day', short=false) }} as day_of_week_name,
    {{ dbt_date.day_name('d.date_day', short=true) }} as day_of_week_name_short,
    {{ dbt_date.day_of_month('d.date_day') }} as day_of_month,
    {{ dbt_date.day_of_year('d.date_day') }} as day_of_year,

    {{ dbt_date.week_start('d.date_day') }} as week_start_date,
    {{ dbt_date.week_end('d.date_day') }} as week_end_date,
    {{ dbt_date.week_start('d.prior_year_over_year_date_day') }} as prior_year_week_start_date,
    {{ dbt_date.week_end('d.prior_year_over_year_date_day') }} as prior_year_week_end_date,
    {{ dbt_date.week_of_year('d.date_day') }} as week_of_year,

    {{ dbt_date.iso_week_start('d.date_day') }} as iso_week_start_date,
    {{ dbt_date.iso_week_end('d.date_day') }} as iso_week_end_date,
    {{ dbt_date.iso_week_start('d.prior_year_over_year_date_day') }} as prior_year_iso_week_start_date,
    {{ dbt_date.iso_week_end('d.prior_year_over_year_date_day') }} as prior_year_iso_week_end_date,
    {{ dbt_date.iso_week_of_year('d.date_day') }} as iso_week_of_year,

    {{ dbt_date.week_of_year('d.prior_year_over_year_date_day') }} as prior_year_week_of_year,
    {{ dbt_date.iso_week_of_year('d.prior_year_over_year_date_day') }} as prior_year_iso_week_of_year,

    cast({{ dbt_date.date_part('month', 'd.date_day') }} as {{ dbt.type_int() }}) as month_of_year,
    {{ dbt_date.month_name('d.date_day', short=false) }}  as month_name,
    {{ dbt_date.month_name('d.date_day', short=true) }}  as month_name_short,

    cast({{ dbt.date_trunc('month', 'd.date_day') }} as date) as month_start_date,
    cast({{ last_day('d.date_day', 'month') }} as date) as month_end_date,

    cast({{ dbt.date_trunc('month', 'd.prior_year_date_day') }} as date) as prior_year_month_start_date,
    cast({{ last_day('d.prior_year_date_day', 'month') }} as date) as prior_year_month_end_date,

    cast({{ dbt_date.date_part('quarter', 'd.date_day') }} as {{ dbt.type_int() }}) as quarter_of_year,
    cast({{ dbt.date_trunc('quarter', 'd.date_day') }} as date) as quarter_start_date,
    cast({{ last_day('d.date_day', 'quarter') }} as date) as quarter_end_date,

    cast({{ dbt_date.date_part('year', 'd.date_day') }} as {{ dbt.type_int() }}) as year_number,
    cast({{ dbt.date_trunc('year', 'd.date_day') }} as date) as year_start_date,
    cast({{ last_day('d.date_day', 'year') }} as date) as year_end_date
from
    dates_with_prior_year_dates d
order by 1
{% endmacro %}

{% macro postgres__get_date_dimension(start_date, end_date) %}
with base_dates as (
    {{ dbt_date.get_base_dates(start_date, end_date) }}
),
dates_with_prior_year_dates as (

    select
        cast(d.date_day as date) as date_day,
        cast({{ dbt.dateadd('year', -1 , 'd.date_day') }} as date) as prior_year_date_day,
        cast({{ dbt.dateadd('day', -364 , 'd.date_day') }} as date) as prior_year_over_year_date_day
    from
    	base_dates d

)
select
    d.date_day,
    {{ dbt_date.yesterday('d.date_day') }} as prior_date_day,
    {{ dbt_date.tomorrow('d.date_day') }} as next_date_day,
    d.prior_year_date_day as prior_year_date_day,
    d.prior_year_over_year_date_day,
    {{ dbt_date.day_of_week('d.date_day', isoweek=true) }} as day_of_week,

    {{ dbt_date.day_name('d.date_day', short=false) }} as day_of_week_name,
    {{ dbt_date.day_name('d.date_day', short=true) }} as day_of_week_name_short,
    {{ dbt_date.day_of_month('d.date_day') }} as day_of_month,
    {{ dbt_date.day_of_year('d.date_day') }} as day_of_year,

    {{ dbt_date.week_start('d.date_day') }} as week_start_date,
    {{ dbt_date.week_end('d.date_day') }} as week_end_date,
    {{ dbt_date.week_start('d.prior_year_over_year_date_day') }} as prior_year_week_start_date,
    {{ dbt_date.week_end('d.prior_year_over_year_date_day') }} as prior_year_week_end_date,
    {{ dbt_date.week_of_year('d.date_day') }} as week_of_year,

    {{ dbt_date.iso_week_start('d.date_day') }} as iso_week_start_date,
    {{ dbt_date.iso_week_end('d.date_day') }} as iso_week_end_date,
    {{ dbt_date.iso_week_start('d.prior_year_over_year_date_day') }} as prior_year_iso_week_start_date,
    {{ dbt_date.iso_week_end('d.prior_year_over_year_date_day') }} as prior_year_iso_week_end_date,
    {{ dbt_date.iso_week_of_year('d.date_day') }} as iso_week_of_year,

    {{ dbt_date.week_of_year('d.prior_year_over_year_date_day') }} as prior_year_week_of_year,
    {{ dbt_date.iso_week_of_year('d.prior_year_over_year_date_day') }} as prior_year_iso_week_of_year,

    cast({{ dbt_date.date_part('month', 'd.date_day') }} as {{ dbt.type_int() }}) as month_of_year,
    {{ dbt_date.month_name('d.date_day', short=false) }}  as month_name,
    {{ dbt_date.month_name('d.date_day', short=true) }}  as month_name_short,

    cast({{ dbt.date_trunc('month', 'd.date_day') }} as date) as month_start_date,
    cast({{ last_day('d.date_day', 'month') }} as date) as month_end_date,

    cast({{ dbt.date_trunc('month', 'd.prior_year_date_day') }} as date) as prior_year_month_start_date,
    cast({{ last_day('d.prior_year_date_day', 'month') }} as date) as prior_year_month_end_date,

    cast({{ dbt_date.date_part('quarter', 'd.date_day') }} as {{ dbt.type_int() }}) as quarter_of_year,
    cast({{ dbt.date_trunc('quarter', 'd.date_day') }} as date) as quarter_start_date,
    {# last_day does not support quarter because postgresql does not support quarter interval. #}
    cast({{dbt.dateadd('day', '-1', dbt.dateadd('month', '3', dbt.date_trunc('quarter', 'd.date_day')))}} as date) as quarter_end_date,

    cast({{ dbt_date.date_part('year', 'd.date_day') }} as {{ dbt.type_int() }}) as year_number,
    cast({{ dbt.date_trunc('year', 'd.date_day') }} as date) as year_start_date,
    cast({{ last_day('d.date_day', 'year') }} as date) as year_end_date
from
    dates_with_prior_year_dates d
order by 1
{% endmacro %}
