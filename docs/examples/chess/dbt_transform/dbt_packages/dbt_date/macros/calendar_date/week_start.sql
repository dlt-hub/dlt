{%- macro week_start(date=None, tz=None) -%}
{%-set dt = date if date else dbt_date.today(tz) -%}
{{ adapter.dispatch('week_start', 'dbt_date') (dt) }}
{%- endmacro -%}

{%- macro default__week_start(date) -%}
cast({{ dbt.date_trunc('week', date) }} as date)
{%- endmacro %}

{%- macro snowflake__week_start(date) -%}
    {#
        Get the day of week offset: e.g. if the date is a Sunday,
        dbt_date.day_of_week returns 1, so we subtract 1 to get a 0 offset
    #}
    {% set off_set = dbt_date.day_of_week(date, isoweek=False) ~ " - 1" %}
    cast({{ dbt.dateadd("day", "-1 * (" ~ off_set ~ ")", date) }} as date)
{%- endmacro %}

{%- macro postgres__week_start(date) -%}
-- Sunday as week start date
cast({{ dbt.dateadd('day', -1, dbt.date_trunc('week', dbt.dateadd('day', 1, date))) }} as date)
{%- endmacro %}

{%- macro duckdb__week_start(date) -%}
{{ return(dbt_date.postgres__week_start(date)) }}
{%- endmacro %}
