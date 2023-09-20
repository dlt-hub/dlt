{%- macro tomorrow(date=None, tz=None) -%}
{{ dbt_date.n_days_away(1, date, tz) }}
{%- endmacro -%}
