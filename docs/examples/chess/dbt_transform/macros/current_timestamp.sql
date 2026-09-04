{# `_dlt_loads` is created by `dlt`, whose snowflake timestamps are `TIMESTAMP_LTZ`. the dbt
   adapter macro returns `TIMESTAMP_TZ`, which snowflake refuses to insert into an LTZ column. #}
{% macro snowflake__current_timestamp() -%}
    current_timestamp()::timestamp_ltz
{%- endmacro %}
