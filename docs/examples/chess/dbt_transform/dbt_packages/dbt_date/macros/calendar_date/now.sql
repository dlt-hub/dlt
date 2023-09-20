{%- macro now(tz=None) -%}
{{ dbt_date.convert_timezone(dbt.current_timestamp(), tz) }}
{%- endmacro -%}
