{% test expression_is_true(model,
                                 expression,
                                 test_condition="= true",
                                 group_by_columns=None,
                                 row_condition=None
                                 ) %}

    {{ dbt_expectations.expression_is_true(model, expression, test_condition, group_by_columns, row_condition) }}

{% endtest %}

{% macro expression_is_true(model,
                                 expression,
                                 test_condition="= true",
                                 group_by_columns=None,
                                 row_condition=None
                                 ) %}
    {{ adapter.dispatch('expression_is_true', 'dbt_expectations') (model, expression, test_condition, group_by_columns, row_condition) }}
{%- endmacro %}

{% macro default__expression_is_true(model, expression, test_condition, group_by_columns, row_condition) -%}
with grouped_expression as (
    select
        {% if group_by_columns %}
        {% for group_by_column in group_by_columns -%}
        {{ group_by_column }} as col_{{ loop.index }},
        {% endfor -%}
        {% endif %}
        {{ dbt_expectations.truth_expression(expression) }}
    from {{ model }}
     {%- if row_condition %}
    where
        {{ row_condition }}
    {% endif %}
    {% if group_by_columns %}
    group by
    {% for group_by_column in group_by_columns -%}
        {{ group_by_column }}{% if not loop.last %},{% endif %}
    {% endfor %}
    {% endif %}

),
validation_errors as (

    select
        *
    from
        grouped_expression
    where
        not(expression {{ test_condition }})

)

select *
from validation_errors


{% endmacro -%}
