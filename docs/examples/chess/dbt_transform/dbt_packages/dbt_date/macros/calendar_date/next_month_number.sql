{%- macro next_month_number(tz=None) -%}
{{ dbt_date.date_part('month', dbt_date.next_month(tz)) }}
{%- endmacro -%}
