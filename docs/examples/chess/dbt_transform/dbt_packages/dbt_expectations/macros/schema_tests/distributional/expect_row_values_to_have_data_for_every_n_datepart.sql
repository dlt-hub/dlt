{%- test expect_row_values_to_have_data_for_every_n_datepart(model,
                                                            date_col,
                                                            date_part="day",
                                                            interval=None,
                                                            row_condition=None,
                                                            exclusion_condition=None,
                                                            test_start_date=None,
                                                            test_end_date=None) -%}
{% if not execute %}
    {{ return('') }}
{% endif %}

{% if not test_start_date or not test_end_date %}
    {% set sql %}

        select
            min({{ date_col }}) as start_{{ date_part }},
            max({{ date_col }}) as end_{{ date_part }}
        from {{ model }}
        {% if row_condition %}
        where {{ row_condition }}
        {% endif %}

    {% endset %}

    {%- set dr = run_query(sql) -%}
    {%- set db_start_date = dr.columns[0].values()[0].strftime('%Y-%m-%d') -%}
    {%- set db_end_date = dr.columns[1].values()[0].strftime('%Y-%m-%d') -%}

{% endif %}

{% if not test_start_date %}
{% set start_date = db_start_date %}
{% else %}
{% set start_date = test_start_date %}
{% endif %}


{% if not test_end_date %}
{% set end_date = db_end_date %}
{% else %}
{% set end_date = test_end_date %}
{% endif %}
with base_dates as (

    {{ dbt_date.get_base_dates(start_date=start_date, end_date=end_date, datepart=date_part) }}
    {% if interval %}
    {#
        Filter the date spine created above down to the interval granularity using a modulo operation.
        The number of date_parts after the start_date divided by the integer interval will produce no remainder for the desired intervals,
        e.g. for 2-day interval from a starting Jan 1, 2020:
            params: start_date = '2020-01-01', date_part = 'day', interval = 2
            date spine created above: [2020-01-01, 2020-01-02, 2020-01-03, 2020-01-04, 2020-01-05, ...]
            The first parameter to the `mod` function would be the number of days between the start_date and the spine date, i.e. [0, 1, 2, 3, 4 ...]
            The second parameter to the `mod` function would be the integer interval, i.e. 2
            This modulo operation produces the following remainders: [0, 1, 0, 1, 0, ...]
            Filtering the spine only where this remainder == 0 will return a spine with every other day as desired, i.e. [2020-01-01, 2020-01-03, 2020-01-05, ...]
    #}
    where mod(
            cast({{ dbt.datediff("'" ~ start_date ~ "'", 'date_' ~ date_part, date_part) }} as {{ dbt.type_int() }}),
            cast({{interval}} as {{ dbt.type_int() }})
        ) = 0
    {% endif %}

),
model_data as (

    select
    {% if not interval %}

        cast({{ dbt.date_trunc(date_part, date_col) }} as {{ dbt_expectations.type_datetime() }}) as date_{{ date_part }},

    {% else %}
        {#
            Use a modulo operator to determine the number of intervals that a date_col is away from the interval-date spine
            and subtracts that amount to effectively slice each date_col record into its corresponding spine bucket,
            e.g. given a date_col of with records [2020-01-01, 2020-01-02, 2020-01-03, 2020-01-11, 2020-01-12]
                if we want to slice these dates into their 2-day buckets starting Jan 1, 2020 (start_date = '2020-01-01', date_part='day', interval=2),
                the modulo operation described above will produce these remainders: [0, 1, 0, 0, 1]
                subtracting that number of days from the observations will produce records [2020-01-01, 2020-01-01, 2020-01-03, 2020-01-11, 2020-01-11],
                all of which align with records from the interval-date spine
        #}
        {{ dbt.dateadd(
            date_part,
            "mod(
                cast(" ~ dbt.datediff("'" ~ start_date ~ "'", date_col, date_part) ~ " as " ~ dbt.type_int() ~ " ),
                cast(" ~ interval ~ " as  " ~ dbt.type_int() ~ " )
            ) * (-1)",
            "cast( " ~ dbt.date_trunc(date_part, date_col) ~ " as  " ~ dbt_expectations.type_datetime() ~ ")"
        )}} as date_{{ date_part }},

    {% endif %}

        count(*) as row_cnt
    from
        {{ model }} f
    {% if row_condition %}
    where {{ row_condition }}
    {% endif %}
    group by
        date_{{date_part}}

),

final as (

    select
        cast(d.date_{{ date_part }} as {{ dbt_expectations.type_datetime() }}) as date_{{ date_part }},
        case when f.date_{{ date_part }} is null then true else false end as is_missing,
        coalesce(f.row_cnt, 0) as row_cnt
    from
        base_dates d
        left join
        model_data f on cast(d.date_{{ date_part }} as {{ dbt_expectations.type_datetime() }}) = f.date_{{ date_part }}
)
select
    *
from final
where row_cnt = 0
{% if exclusion_condition %}
  and {{ exclusion_condition }}
{% endif %}
{%- endtest -%}
