{%- macro iso_week_start(date=None, tz=None) -%}
{%-set dt = date if date else dbt_date.today(tz) -%}
{{ adapter.dispatch('iso_week_start', 'dbt_date') (dt) }}
{%- endmacro -%}

{%- macro _iso_week_start(date, week_type) -%}
cast({{ dbt.date_trunc(week_type, date) }} as date)
{%- endmacro %}

{%- macro default__iso_week_start(date) -%}
{{ dbt_date._iso_week_start(date, 'isoweek') }}
{%- endmacro %}

{%- macro snowflake__iso_week_start(date) -%}
{{ dbt_date._iso_week_start(date, 'week') }}
{%- endmacro %}

{%- macro postgres__iso_week_start(date) -%}
{{ dbt_date._iso_week_start(date, 'week') }}
{%- endmacro %}

{%- macro duckdb__iso_week_start(date) -%}
{{ return(dbt_date.postgres__iso_week_start(date)) }}
{%- endmacro %}
