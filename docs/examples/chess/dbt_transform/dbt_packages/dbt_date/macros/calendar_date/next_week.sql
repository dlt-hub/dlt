{%- macro next_week(tz=None) -%}
{{ dbt_date.n_weeks_away(1, tz) }}
{%- endmacro -%}