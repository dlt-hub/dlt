{%- macro iso_week_end(date=None, tz=None) -%}
{%-set dt = date if date else dbt_date.today(tz) -%}
{{ adapter.dispatch('iso_week_end', 'dbt_date') (dt) }}
{%- endmacro -%}

{%- macro _iso_week_end(date, week_type) -%}
{%- set dt = dbt_date.iso_week_start(date) -%}
{{ dbt_date.n_days_away(6, dt) }}
{%- endmacro %}

{%- macro default__iso_week_end(date) -%}
{{ dbt_date._iso_week_end(date, 'isoweek') }}
{%- endmacro %}

{%- macro snowflake__iso_week_end(date) -%}
{{ dbt_date._iso_week_end(date, 'weekiso') }}
{%- endmacro %}

