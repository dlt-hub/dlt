{%- macro last_month(tz=None) -%}
{{ dbt_date.n_months_ago(1, tz) }}
{%- endmacro -%}