{%- macro next_month_name(short=True, tz=None) -%}
{{ dbt_date.month_name(dbt_date.next_month(tz), short=short) }}
{%- endmacro -%}
