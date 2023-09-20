{%- macro next_month(tz=None) -%}
{{ dbt_date.n_months_away(1, tz) }}
{%- endmacro -%}