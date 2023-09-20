{% macro get_fiscal_periods(dates, year_end_month, week_start_day, shift_year=1) %}
{#
This macro requires you to pass in a ref to a date dimension, created via
dbt_date.get_date_dimension()s
#}
with fscl_year_dates_for_periods as (
    {{ dbt_date.get_fiscal_year_dates(dates, year_end_month, week_start_day, shift_year) }}
),
fscl_year_w13 as (

    select
        f.*,
        -- We count the weeks in a 13 week period
        -- and separate the 4-5-4 week sequences
        mod(cast(
            (f.fiscal_week_of_year-1) as {{ dbt.type_int() }}
            ), 13) as w13_number,
        -- Chop weeks into 13 week merch quarters
        cast(
            least(
                floor((f.fiscal_week_of_year-1)/13.0)
                , 3)
            as {{ dbt.type_int() }}) as quarter_number
    from
        fscl_year_dates_for_periods f

),
fscl_periods as (

    select
        f.date_day,
        f.fiscal_year_number,
        f.week_start_date,
        f.week_end_date,
        f.fiscal_week_of_year,
        case
            -- we move week 53 into the 3rd period of the quarter
            when f.fiscal_week_of_year = 53 then 3
            when f.w13_number between 0 and 3 then 1
            when f.w13_number between 4 and 8 then 2
            when f.w13_number between 9 and 12 then 3
        end as period_of_quarter,
        f.quarter_number
    from
        fscl_year_w13 f

),
fscl_periods_quarters as (

    select
        f.*,
        cast((
            (f.quarter_number * 3) + f.period_of_quarter
         ) as {{ dbt.type_int() }}) as fiscal_period_number
    from
        fscl_periods f

)
select
    date_day,
    fiscal_year_number,
    week_start_date,
    week_end_date,
    fiscal_week_of_year,
    dense_rank() over(partition by fiscal_period_number order by fiscal_week_of_year) as fiscal_week_of_period,
    fiscal_period_number,
    quarter_number+1 as fiscal_quarter_number,
    period_of_quarter as fiscal_period_of_quarter
from
    fscl_periods_quarters
order by 1,2
{% endmacro %}
