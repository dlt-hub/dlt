{% macro round_timestamp(timestamp) %}
    {{ dbt.date_trunc("day", dbt.dateadd("hour", 12, timestamp)) }}
{% endmacro %}
