{%- macro last_week(tz=None) -%}
{{ dbt_date.n_weeks_ago(1, tz) }}
{%- endmacro -%}