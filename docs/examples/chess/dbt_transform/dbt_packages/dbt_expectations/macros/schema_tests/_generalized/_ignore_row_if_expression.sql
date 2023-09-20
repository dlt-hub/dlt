
{% macro ignore_row_if_expression(ignore_row_if, columns) %}
    {{ adapter.dispatch('ignore_row_if_expression', 'dbt_expectations') (ignore_row_if, columns) }}
{% endmacro %}

{% macro default__ignore_row_if_expression(ignore_row_if, columns) %}
  {%- set ignore_row_if_values = ["all_values_are_missing", "any_value_is_missing"] -%}
    {% if ignore_row_if not in ignore_row_if_values %}
        {{ exceptions.raise_compiler_error(
            "`ignore_row_if` must be one of " ~ (ignore_row_if_values | join(", ")) ~ ". Got: '" ~ ignore_row_if ~"'.'"
        ) }}
    {% endif %}

    {%- set op = "and" if ignore_row_if == "all_values_are_missing" else "or" -%}
    not (
        {% for column in columns -%}
        {{ column }} is null{% if not loop.last %} {{ op }} {% endif %}
        {% endfor %}
    )
{% endmacro %}
