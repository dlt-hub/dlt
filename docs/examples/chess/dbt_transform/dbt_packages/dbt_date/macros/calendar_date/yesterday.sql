{%- macro yesterday(date=None, tz=None) -%}
{{ dbt_date.n_days_ago(1, date, tz) }}
{%- endmacro -%}
