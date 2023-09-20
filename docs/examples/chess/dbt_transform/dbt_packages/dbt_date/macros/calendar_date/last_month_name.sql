{%- macro last_month_name(short=True, tz=None) -%}
{{ dbt_date.month_name(dbt_date.last_month(tz), short=short) }}
{%- endmacro -%}
