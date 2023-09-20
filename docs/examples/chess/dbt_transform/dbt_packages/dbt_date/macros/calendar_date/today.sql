{%- macro today(tz=None) -%}
cast({{ dbt_date.now(tz) }} as date)
{%- endmacro -%}
