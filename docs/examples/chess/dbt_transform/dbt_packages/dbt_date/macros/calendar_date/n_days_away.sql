{%- macro n_days_away(n, date=None, tz=None) -%}
{{ dbt_date.n_days_ago(-1 * n, date, tz) }}
{%- endmacro -%}