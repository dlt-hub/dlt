{%- macro last_month_number(tz=None) -%}
{{ dbt_date.date_part('month', dbt_date.last_month(tz)) }}
{%- endmacro -%}
