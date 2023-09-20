
{% macro _get_like_pattern_expression(column_name, like_pattern, positive) %}
{{ column_name }} {{ "not" if not positive else "" }} like '{{ like_pattern }}'
{% endmacro %}
