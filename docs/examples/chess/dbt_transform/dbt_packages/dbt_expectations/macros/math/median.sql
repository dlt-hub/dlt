{% macro median(field) %}
{{ dbt_expectations.percentile_cont(field, 0.5) }}
{% endmacro %}
