{%- macro iso_week_of_year(date=None, tz=None) -%}
{%-set dt = date if date else dbt_date.today(tz) -%}
{{ adapter.dispatch('iso_week_of_year', 'dbt_date') (dt) }}
{%- endmacro -%}

{%- macro _iso_week_of_year(date, week_type) -%}
cast({{ dbt_date.date_part(week_type, date) }} as {{ dbt.type_int() }})
{%- endmacro %}

{%- macro default__iso_week_of_year(date) -%}
{{ dbt_date._iso_week_of_year(date, 'isoweek') }}
{%- endmacro %}

{%- macro snowflake__iso_week_of_year(date) -%}
{{ dbt_date._iso_week_of_year(date, 'weekiso') }}
{%- endmacro %}

{%- macro postgres__iso_week_of_year(date) -%}
-- postgresql week is isoweek, the first week of a year containing January 4 of that year.
{{ dbt_date._iso_week_of_year(date, 'week') }}
{%- endmacro %}

{%- macro duckdb__iso_week_of_year(date) -%}
{{ return(dbt_date.postgres__iso_week_of_year(date)) }}
{%- endmacro %}
