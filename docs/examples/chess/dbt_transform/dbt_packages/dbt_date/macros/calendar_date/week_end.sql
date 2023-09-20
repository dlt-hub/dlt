{%- macro week_end(date=None, tz=None) -%}
{%-set dt = date if date else dbt_date.today(tz) -%}
{{ adapter.dispatch('week_end', 'dbt_date') (dt) }}
{%- endmacro -%}

{%- macro default__week_end(date) -%}
{{ last_day(date, 'week') }}
{%- endmacro %}

{%- macro snowflake__week_end(date) -%}
{%- set dt = dbt_date.week_start(date) -%}
{{ dbt_date.n_days_away(6, dt) }}
{%- endmacro %}

{%- macro postgres__week_end(date) -%}
{%- set dt = dbt_date.week_start(date) -%}
{{ dbt_date.n_days_away(6, dt) }}
{%- endmacro %}

{%- macro duckdb__week_end(date) -%}
{{ return(dbt_date.postgres__week_end(date)) }}
{%- endmacro %}
