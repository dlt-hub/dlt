{% macro get_fiscal_year_dates(dates, year_end_month=12, week_start_day=1, shift_year=1) %}
{{ adapter.dispatch('get_fiscal_year_dates', 'dbt_date') (dates, year_end_month, week_start_day, shift_year) }}
{% endmacro %}

{% macro default__get_fiscal_year_dates(dates, year_end_month, week_start_day, shift_year) %}
-- this gets all the dates within a fiscal year
-- determined by the given year-end-month
-- ending on the saturday closest to that month's end date
with date_dimension as (
    select * from {{ dates }}
),
year_month_end as (

    select
       d.year_number - {{ shift_year }} as fiscal_year_number,
       d.month_end_date
    from
        date_dimension d
    where
        d.month_of_year = {{ year_end_month }}
    group by 1,2

),
weeks as (

    select
        d.year_number,
        d.month_of_year,
        d.date_day as week_start_date,
        cast({{ dbt.dateadd('day', 6, 'd.date_day') }} as date) as week_end_date
    from
        date_dimension d
    where
        d.day_of_week = {{ week_start_day }}

),
-- get all the weeks that start in the month the year ends
year_week_ends as (

    select
        d.year_number - {{ shift_year }} as fiscal_year_number,
        d.week_end_date
    from
        weeks d
    where
        d.month_of_year = {{ year_end_month }}
    group by
        1,2

),
-- then calculate which Saturday is closest to month end
weeks_at_month_end as (

    select
        d.fiscal_year_number,
        d.week_end_date,
        m.month_end_date,
        rank() over
            (partition by d.fiscal_year_number
                order by
                abs({{ dbt.datediff('d.week_end_date', 'm.month_end_date', 'day') }})

            ) as closest_to_month_end
    from
        year_week_ends d
        join
        year_month_end m on d.fiscal_year_number = m.fiscal_year_number
),
fiscal_year_range as (

    select
        w.fiscal_year_number,
        cast(
            {{ dbt.dateadd('day', 1,
            'lag(w.week_end_date) over(order by w.week_end_date)') }}
            as date) as fiscal_year_start_date,
        w.week_end_date as fiscal_year_end_date
    from
        weeks_at_month_end w
    where
        w.closest_to_month_end = 1

),
fiscal_year_dates as (

    select
        d.date_day,
        m.fiscal_year_number,
        m.fiscal_year_start_date,
        m.fiscal_year_end_date,
        w.week_start_date,
        w.week_end_date,
        -- we reset the weeks of the year starting with the merch year start date
        dense_rank()
            over(
                partition by m.fiscal_year_number
                order by w.week_start_date
                ) as fiscal_week_of_year
    from
        date_dimension d
        join
        fiscal_year_range m on d.date_day between m.fiscal_year_start_date and m.fiscal_year_end_date
        join
        weeks w on d.date_day between w.week_start_date and w.week_end_date

)
select * from fiscal_year_dates order by 1
{% endmacro %}
