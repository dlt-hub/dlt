{% test expect_column_distinct_count_to_equal_other_table(model,
                                                                compare_model,
                                                                column_name,
                                                                compare_column_name,
                                                                row_condition=None,
                                                                compare_row_condition=None
                                                                ) %}
{%- set expression -%}
count(distinct {{ column_name }})
{%- endset -%}
{%- set compare_expression -%}
{%- if compare_column_name -%}
count(distinct {{ compare_column_name }})
{%- else -%}
{{ expression }}
{%- endif -%}
{%- endset -%}
{{ dbt_expectations.test_equal_expression(
    model,
    expression=expression,
    compare_model=compare_model,
    compare_expression=compare_expression,
    row_condition=row_condition,
    compare_row_condition=compare_row_condition
) }}
{%- endtest -%}
