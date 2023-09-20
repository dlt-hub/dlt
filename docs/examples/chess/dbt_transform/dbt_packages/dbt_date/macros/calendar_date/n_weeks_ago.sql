{%- macro n_weeks_ago(n, tz=None) -%}
{%- set n = n|int -%}
{{ dbt.date_trunc('week',
    dbt.dateadd('week', -1 * n,
        dbt_date.today(tz)
        )
    ) }}
{%- endmacro -%}
